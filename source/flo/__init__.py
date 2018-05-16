#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_csrb_monthly package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
from calendar import monthrange
import logging
import traceback

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_csrb_daily as hirs_csrb_daily
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)


class HIRS_CSRB_MONTHLY(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id']
    outputs = ['stats', 'zonal_means']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id}
                for g in granules]

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CSRB_MONTHLY')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        LOG.debug("Running build_task()")
        LOG.debug("context:  {}".format(context))

        # Instantiate the hirs_csrb_daily computation
        hirs_csrb_daily_comp = hirs_csrb_daily.HIRS_CSRB_DAILY()

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'],
                                context['granule'] + timedelta(num_days),
                                False, True)
        daily_contexts = hirs_csrb_daily_comp.find_contexts(
                                                            interval,
                                                            context['satellite'],
                                                            context['hirs2nc_delivery_id'],
                                                            context['hirs_avhrr_delivery_id'],
                                                            context['hirs_csrb_daily_delivery_id'])

        if len(daily_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CSRB_DAILY inputs available for {}'.format(context['granule']))

        SPC = StoredProductCatalog()

        for (i, daily_context) in enumerate(daily_contexts):
            hirs_csrb_daily_prod = hirs_csrb_daily_comp.dataset('means').product(daily_context)
            if SPC.exists(hirs_csrb_daily_prod):
                task.input('CSRB_DAILY-{}'.format(i), hirs_csrb_daily_prod, True)

    def create_inputs_filelist(self, inputs):
        '''
        Create a text file containing the full paths of the input daily CFSR files.
        '''

        rc = 0

        try:
            csrb_list = 'csrb_daily_means_filelist'
            with open(csrb_list, 'w') as f:
                [f.write('{}\n'.format(basename(input))) for input in sorted(inputs.values())]
        except CalledProcessError as err:
            rc = err.returncode
            LOG.error("Writing of csrb_daily_means_filelist failed with a return value of {}".format(rc))
            return rc, None

        return rc, csrb_list

    def create_monthly_statistics(self, inputs, context):
        '''
        Create the CFSR statistics for the current month.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_csrb_monthly_delivery_id = context['hirs_csrb_monthly_delivery_id']
        delivery = delivered_software.lookup('hirs_csrb_monthly', delivery_id=hirs_csrb_monthly_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        version = delivery.version

        # Link the input files into the working directory
        _ = link_files(current_dir, inputs.values())

        # Create a text file containing the input daily files.
        rc, inputs_filelist = self.create_inputs_filelist(inputs)
        if rc != 0:
            return rc, None

        # Determine the output filenames
        output_stats = 'csrb_monthly_stats_{}_{}.nc'.format(context['satellite'],
                                                            context['granule'].strftime('d%Y%m'))
        LOG.debug("output_stats: {}".format(output_stats))

        csrb_monthly_stats_bin = pjoin(dist_root, 'bin/create_monthly_global_csrbs_netcdf.exe')

        cmd = '{0:} {1:} {2:} > {2:}.log'.format(
                csrb_monthly_stats_bin,
                inputs_filelist,
                output_stats
                )
        #cmd = 'sleep 1; touch {}'.format(output_stats)

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_csrb_cfsr = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_csrb_cfsr = err.returncode
            LOG.error("CSRB monthly binary {} returned a value of {}".format(csrb_monthly_stats_bin, rc_csrb_cfsr))
            return rc_csrb_cfsr, None

        # Verify output file
        output_stats = glob(output_stats)
        if len(output_stats) != 0:
            output_stats = output_stats[0]
            LOG.info('Found output CFSR monthly statistics file "{}"'.format(output_stats))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_stats))
            rc = 1
            return rc, None

        return rc, output_stats

    def create_monthly_zonal_means(self, stats_file, context):
        '''
        Create the CFSR statistics for the current month.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_csrb_monthly_delivery_id = context['hirs_csrb_monthly_delivery_id']
        delivery = delivered_software.lookup('hirs_csrb_monthly', delivery_id=hirs_csrb_monthly_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        version = delivery.version

        # Determine the output filenames
        output_zonal_means = basename(stats_file).replace('stats', 'zmeans')
        LOG.debug("output_zonal_means: {}".format(output_zonal_means))

        csrb_zonal_means_bin = pjoin(dist_root, 'bin/create_monthly_zonal_csrbs_netcdf.exe')

        cmd = '{0:} {1:} {2:} > {2:}.log'.format(
                csrb_zonal_means_bin,
                stats_file,
                output_zonal_means
                )
        #cmd = 'sleep 1; touch {}'.format(output_zonal_means)

        LOG.debug('\n'+cmd+'\n')

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_csrb_cfsr = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_csrb_cfsr = err.returncode
            LOG.error("CSRB monthly binary {} returned a value of {}".format(csrb_zonal_means_bin, rc_csrb_cfsr))
            return rc_csrb_cfsr, None

        # Verify output file
        output_zonal_means = glob(output_zonal_means)
        if len(output_zonal_means) != 0:
            output_zonal_means = output_zonal_means[0]
            LOG.info('Found output CFSR monthly zonal means file "{}"'.format(output_zonal_means))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_zonal_means))
            rc = 1
            return rc, None

        return rc, output_zonal_means

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CSRB_MONTHLY')
    def run_task(self, inputs, context):

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Create the CFSR statistics for the current month.
        rc, output_stats_file = self.create_monthly_statistics(inputs, context)
        if rc != 0:
            return rc
        LOG.info('create_monthly_statistics() generated {}...'.format(output_stats_file))

        # Create the CFSR zonal means for the current month
        rc, output_zonal_means_file = self.create_monthly_zonal_means(output_stats_file, context)
        if rc != 0:
            return rc
        LOG.info('create_zonal_means() generated {}...'.format(output_zonal_means_file))

        return {'stats': nc_compress(output_stats_file), 'zonal_means': nc_compress(output_zonal_means_file)}
