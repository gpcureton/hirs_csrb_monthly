#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_csrb_monthly package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""


import sys
import traceback
import calendar
import logging
from calendar import monthrange
from time import sleep

from flo.ui import safe_submit_order
from timeutil import TimeInterval, datetime, timedelta

import flo.sw.hirs_csrb_daily as hirs_csrb_daily
import flo.sw.hirs_csrb_monthly as hirs_csrb_monthly
from flo.sw.hirs2nc.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

setup_logging(2)

# General information
hirs2nc_delivery_id = '20180410-1'
hirs_avhrr_delivery_id = '20180505-1'
hirs_csrb_daily_delivery_id = '20180511-1'
hirs_csrb_monthly_delivery_id  = '20180516-1'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information

#satellite = 'noaa-19'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + day - wedge)]
# NSS.GHRR.NP.D09108.S2301.E0050.B0100809.GC.level2.hdf --> NSS.GHRR.NP.D17365.S2238.E2359.B4585757.GC.level2.hdf
# datetime(2009, 4, 18, 0, 0) --> datetime(2017, 12, 31, 0, 0)
#intervals = [TimeInterval(datetime(years,1,1), datetime(years+1,1,1)-wedge) for years in range(2009, 2018) ]

satellite = 'metop-b'
#intervals = [TimeInterval(datetime(2015,1,1), datetime(2015,2,1)-wedge)]
# NSS.GHRR.M1.D13140.S0029.E0127.B0347172.SV.level2.hdf --> NSS.GHRR.M1.D15365.S2307.E0004.B1705253.SV.level2.hdf
# datetime(2013, 5, 20, 0, 0) --> datetime(2017, 12, 31, 0, 0)
intervals = [TimeInterval(datetime(years,1,1), datetime(years+1,1,1)-wedge) for years in range(2013, 2018) ]

satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

# Instantiate the computation
hirs_csrb_daily_comp = hirs_csrb_daily.HIRS_CSRB_DAILY()
comp = hirs_csrb_monthly.HIRS_CSRB_MONTHLY()

LOG.info("Submitting intervals...")

dt = datetime.utcnow()
log_name = 'hirs_csrb_monthly_{}_s{}_e{}_c{}.log'.format(
    satellite,
    intervals[0].left.strftime('%Y%m'),
    intervals[-1].right.strftime('%Y%m'),
    dt.strftime('%Y%m%d%H%M%S'))

try:

    for interval in intervals:
        LOG.info("Submitting interval {} -> {}".format(interval.left, interval.right))

        contexts = comp.find_contexts(interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id, hirs_csrb_daily_delivery_id,
                          hirs_csrb_monthly_delivery_id)

        LOG.info("Opening log file {}".format(log_name))
        file_obj = open(log_name,'a')

        LOG.info("\tThere are {} contexts in this interval".format(len(contexts)))
        contexts.sort()

        if contexts != []:
            #for context in contexts:
                #LOG.info(context)

            LOG.info("\tFirst context: {}".format(contexts[0]))
            LOG.info("\tLast context:  {}".format(contexts[-1]))

            try:
                job_nums = []
                job_nums = safe_submit_order(comp, [comp.dataset('zonal_means')], contexts, download_onlies=[hirs_csrb_daily_comp])

                if job_nums != []:
                    #job_nums = range(len(contexts))
                    #LOG.info("\t{}".format(job_nums))

                    file_obj.write("contexts: [{}, {}]; job numbers: {{{}..{}}}\n".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("contexts: [{}, {}]; job numbers: {{{},{}}}".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("job numbers: {{{}..{}}}\n".format(job_nums[0],job_nums[-1]))
                else:
                    LOG.info("contexts: {{{}, {}}}; --> no jobs".format(contexts[0], contexts[-1]))
                    file_obj.write("contexts: {{{}, {}}}; --> no jobs\n".format(contexts[0], contexts[-1]))
            except Exception:
                LOG.warning(traceback.format_exc())

            #sleep(30.)

        LOG.info("Closing log file {}".format(log_name))
        file_obj.close()

except Exception:
    LOG.warning(traceback.format_exc())
