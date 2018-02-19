
from calendar import monthrange
from datetime import datetime, timedelta
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import TimeInterval
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs_csrb_daily import HIRS_CSRB_DAILY

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__name__)


class HIRS_CSRB_MONTHLY(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version']
    outputs = ['stats', 'zonal_means']

    def build_task(self, context, task):

        num_days = monthrange(context['granule'].year, context['granule'].month)[1]
        interval = TimeInterval(context['granule'], context['granule'] + timedelta(num_days),
                                False, True)
        daily_contexts = HIRS_CSRB_DAILY().find_contexts(context['sat'], context['hirs_version'],
                                                         context['collo_version'],
                                                         context['csrb_version'],
                                                         interval)

        for (i, c) in enumerate(daily_contexts):
            task.input('CSRB_DAILY-{}'.format(i), HIRS_CSRB_DAILY().dataset('means').product(c),
                       True)

    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)

        if len(inputs) == 0:
            #raise Exception("NO CSRB DAILY INPUTS PROVIDED")
            raise WorkflowNotReady('No HIRS_CSRB_DAILY inputs available for {}'.format(context['granule']))

        lib_dir = os.path.join(self.package_root, context['csrb_version'], 'lib')

        output_stats = 'csrb_monthly_stats_{}_{}.nc'.format(context['sat'],
                                                            context['granule'].strftime('D%y%j'))
        output_zm = 'csrb_monthly_zmeans_{}_{}.nc'.format(context['sat'],
                                                          context['granule'].strftime('D%y%j'))

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

    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, time_interval):

        granules = []

        start = datetime(time_interval.left.year, time_interval.left.month, 1)
        end = datetime(time_interval.right.year, time_interval.right.month, 1)
        date = start

        while date <= end:
            granules.append(date)
            date = date + timedelta(days=monthrange(date.year, date.month)[1])

        return [{'granule': g, 'sat': sat, 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version}
                for g in granules]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'CSRB_MONTHLY')
