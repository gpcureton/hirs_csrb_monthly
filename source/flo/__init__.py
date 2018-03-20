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
    #delivered_software,
    #support_software,
    #runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs_csrb_daily as hirs_csrb_daily

# every module should have a LOG object
LOG = logging.getLogger(__name__)


class HIRS_CSRB_MONTHLY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version']
    outputs = ['stats', 'zonal_means']

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
                                                            context['sat'],
                                                            context['hirs_version'],
                                                            context['collo_version'],
                                                            context['csrb_version'])

        if len(daily_contexts) == 0:
            raise WorkflowNotReady('No HIRS_CSRB_DAILY inputs available for {}'.format(context['granule']))

        SPC = StoredProductCatalog()

        for (i, daily_context) in enumerate(daily_contexts):
            hirs_csrb_daily_prod = hirs_csrb_daily_comp.dataset('means').product(daily_context)
            if SPC.exists(hirs_csrb_daily_prod):
                task.input('CSRB_DAILY-{}'.format(i), hirs_csrb_daily_prod, True)

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_CSRB_MONTHLY')
    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)

        if len(inputs) == 0:
            raise WorkflowNotReady('No HIRS_CSRB_DAILY inputs available for {}'.format(context['granule']))

        lib_dir = os.path.join(self.package_root, context['csrb_version'], 'lib')

        output_stats = 'csrb_monthly_stats_{}_{}.nc'.format(context['sat'],
                                                            context['granule'].strftime('d%Y%m'))
        output_zm = 'csrb_monthly_zmeans_{}_{}.nc'.format(context['sat'],
                                                            context['granule'].strftime('d%Y%m'))

        # Generating CSRB Daily Input List
        csrb_list = 'csrb_daily_means_filelist'
        with open(csrb_list, 'w') as f:
            [f.write('{}\n'.format(input)) for input in inputs.values()]

        # Generating Monthly Stats
        cmd = os.path.join(self.package_root, context['csrb_version'],
                           'bin/create_monthly_global_csrbs_netcdf.exe')
        cmd += ' {} {}'.format(csrb_list, output_stats)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        # Generating Zonal Means
        cmd = os.path.join(self.package_root, context['csrb_version'],
                           'bin/create_monthly_zonal_csrbs_netcdf.exe')
        cmd += ' {} {}'.format(output_stats, output_zm)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'stats': output_stats, 'zonal_means': output_zm}

    def find_contexts(self, time_interval, sat, hirs_version, collo_version, csrb_version):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CSRB_MONTHLY')
