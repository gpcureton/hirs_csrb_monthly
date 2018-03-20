#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_csrb_monthly package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import sys
import traceback
import logging

from timeutil import TimeInterval, datetime, timedelta
from flo.ui import local_prepare, local_execute

import flo.sw.hirs_csrb_daily as hirs_csrb_daily
import flo.sw.hirs_csrb_monthly as hirs_csrb_monthly
from flo.sw.hirs.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

# General information
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
wedge = timedelta(seconds=1.)

# Satellite specific information
#satellite = 'noaa-19'
satellite = 'metop-b'

# Instantiate the computations
hirs_csrb_daily_comp = hirs_csrb_daily.HIRS_CSRB_DAILY()
comp = hirs_csrb_monthly.HIRS_CSRB_MONTHLY()

#
# Local execution
#

def local_execute_example(interval, satellite, hirs_version, collo_version, csrb_version,
                          skip_prepare=False, skip_execute=False, verbosity=2):
    setup_logging(verbosity)

    # Get the required context...
    LOG.info("Getting required contexts for local execution...")
    contexts =  comp.find_contexts(interval, satellite, hirs_version, collo_version, csrb_version)
    LOG.info("Finished getting required contexts for local execution\n")

    if len(contexts) != 0:
        LOG.info("Candidate contexts in interval...")
        for context in contexts:
            print("\t{}".format(context))

        try:
            if not skip_prepare:
                LOG.info("Running hirs_csrb_monthly local_prepare()...")
                LOG.info("Preparing context... {}".format(contexts[0]))
                local_prepare(comp, contexts[0],download_only=[hirs_csrb_daily_comp])
            if not skip_execute:
                LOG.info("Running hirs_csrb_monthly local_execute()...")
                LOG.info("Running context... {}".format(contexts[0]))
                local_execute(comp, contexts[0])
        except Exception, err:
            LOG.error("{}".format(err))
            LOG.debug(traceback.format_exc())
    else:
        LOG.error("There are no valid {} contexts for the interval {}.".format(satellite, interval))

def print_contexts(interval, satellite, hirs_version, collo_version, csrb_version, verbosity=2):
    setup_logging(verbosity)
    contexts = comp.find_contexts(interval, satellite, hirs_version, collo_version, csrb_version)
    for context in contexts:
        LOG.info(context)

#satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    #'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    #'noaa-19', 'metop-a', 'metop-b']

#local_execute_example(granule, satellite, hirs_version, collo_version, csrb_version):
