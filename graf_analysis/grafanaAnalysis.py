from django.db import models
import datetime as dt
import time
import os, sys, traceback, operator
import numpy as np
import pandas as pd
from sosdb import Sos
from sosdb.DataSet import DataSet
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform

LOG_FILE = "/var/www/ovis_web_svcs/sosgui.log"
LOG_DATE_FMT = "%F %T"

# Log class for debugging
class MsgLog(object):
    def __init__(self, prefix):
        self.prefix = prefix
        self.fp = open(LOG_FILE, 'a')
        self.dt_fmt = LOG_DATE_FMT

    def write(self, obj):
        now = dt.datetime.now()
        pfx = now.strftime(self.dt_fmt)
        pfx = pfx + ":" + self.prefix + ":"
        if isinstance(obj, Exception):
            exc_type, exc_obj, exc_tb = sys.exc_info()
            self.fp.write(pfx + "\n")
            traceback.print_exception(exc_type, exc_obj, exc_tb, 10, self.fp)
        else:
            self.fp.write(pfx + str(obj) + "\n")
        self.fp.flush()

    def __del__(self):
        if self.fp:
            self.fp.close()
        self.fp = None

    def close(self):
        if self.fp:
            self.fp.close()
        self.fp = None

# Base class for grafana analysis modules
class Analysis(object):
    def __init__(self, cont, start, end, schema=None, maxDataPoints=4096):
        self.cont = cont
        self.schema = schema
        self.start = float(start)
        self.end = float(end)
        self.query = Sos.SqlQuery(cont, maxDataPoints)
        self.mdp = maxDataPoints

    def get_all_data(self, query):
        df = query.next()
        if df is None:
           return None
        res = df.copy(deep=True)
        while df is not None:
            df = query.next()
            res = pd.concat([df, res])
        return res

    def select_clause(self, metrics):
        select = f'select {",".join(metrics)} from {self.schema}'
        return select

    def get_where(self, filters):
        where_clause = f'where (timestamp > {self.start}) and (timestamp < {self.end})'
        for filt in filters:
            where_clause += f' and ({filt})'
        return where_clause

    def parse_params(self, params):
        if params is None:
            self.threshold = 5
        else:
            if 'threshold' in params:
                self.threshold = int(params.split('=')[1])
            else:
                self.threshold = 5

            if 'idle' in params:
                self.idle = True
            else:
                self.idle = False

            if 'summary' in params:
                self.summary = True
            else:
                self.summary = False

            if 'meta' in params:
                self._meta = True
            else:
                self._meta = False

            if 'bins' in params:
                self.bins = int(params.split('=')[1])
            else:
                self.bins = 20

class papiAnalysis(Analysis):
    def __init__(self, cont, start, end, schema=None, maxDataPoints=1000000):
        super().__init__(cont, start, end, schema=schema, maxDataPoints=maxDataPoints)
        self.mdp = maxDataPoints
        self.job_status_str = {
            1 : "running",
            2 : "complete"
        }
        self.papi_metrics = [
            "PAPI_TOT_INS",
            "PAPI_TOT_CYC",
            "PAPI_LD_INS",
            "PAPI_SR_INS",
            "PAPI_BR_INS",
            "PAPI_FP_OPS",
            "PAPI_L1_ICM",
            "PAPI_L1_DCM",
            "PAPI_L2_ICA",
            "PAPI_L2_TCA",
            "PAPI_L2_TCM",
            "PAPI_L3_TCA",
            "PAPI_L3_TCM"
        ]
        self.event_name_map = {
            "PAPI_TOT_INS" : "tot_ins",
            "PAPI_TOT_CYC" : "tot_cyc",
            "PAPI_LD_INS"  : "ld_ins",
            "PAPI_SR_INS"  : "sr_ins",
            "PAPI_BR_INS"  : "br_ins",
            "PAPI_FP_OPS"  : "fp_ops",
            "PAPI_L1_ICM"  : "l1_icm",
            "PAPI_L1_DCM"  : "l1_dcm",
            "PAPI_L2_ICA"  : "l2_ica",
            "PAPI_L2_TCA"  : "l2_tca",
            "PAPI_L2_TCM"  : "l2_tcm",
            "PAPI_L3_TCA"  : "l3_tca",
            "PAPI_L3_TCM"  : "l3_tcm"
        }
        self.papi_derived_metrics = {
            "cpi" : "cpi",
            "uopi" : "uopi",
            "l1_miss_rate" : "l1_miss_rate",
            "l1_miss_ratio" : "l1_miss_ratio",
            "l2_miss_rate" : "l2_miss_rate",
            "l2_miss_ratio" : "l2_miss_ratio",
            "l3_miss_rate" : "l3_miss_rate",
            "l3_miss_ratio" : "l3_miss_ratio",
            "l2_bw" : "l2_bw",
            "l3_bw" : "l3_bw",
            "fp_rate" : "fp_rate",
            "branch_rate" : "branch_rate",
            "load_rate" : "load_rate",
            "store_rate" : "store_rate"
        }

    def derived_metrics(self, job_id):
        """Calculate derived papi metrics for a given job_id"""
        try:
            self.derived_names = [ "tot_ins", "tot_cyc", "ld_ins", "sr_ins", "br_ins",
                                   "fp_ops", "l1_icm", "l1_dcm", "l2_ica", "l2_tca",
                                   "l2_tcm", "l3_tca", "l3_tcm" ]
            src = SosDataSource()
            src.config(cont=self.cont)
            src.select(
                [ 'PAPI_TOT_INS[timestamp]',
                  'PAPI_TOT_INS[component_id]',
                  'PAPI_TOT_INS[job_id]',
                  'PAPI_TOT_INS[rank]' ] + list(self.event_name_map.keys()),
                       from_    = list(self.event_name_map.keys()),
                       where    = [ [ 'job_id', Sos.COND_EQ, int(job_id) ]
                                ],
                       order_by = 'job_rank_time')

            xfrm = Transform(src, None)
            res = xfrm.begin()
            if res is None:
                # Job was too short to record data
                return (None, None)

            while res is not None:
                res = next(xfrm)
                if res is not None:
                    # concatenate TOP and TOP~1
                    xfrm.concat()

            # result now on top of stack
            result = xfrm.pop()                  # result on top
            # "Normalize" the event names
            for name in self.event_name_map:
                result.rename(name, self.event_name_map[name])

            xfrm.push(result)
            job = xfrm.pop()

            # cpi = tot_cyc / tot_ins
            job <<= job['tot_cyc'] / job['tot_ins'] >> 'cpi'

            # memory accesses
            mem_acc = job['ld_ins'] + job['sr_ins'] >> 'mem_acc'

            # uopi = (ld_ins + sr_ins) / tot_ins
            job <<= mem_acc / job['tot_ins'] >> 'uopi'

            # l1_miss_rate = (l1_icm + l1_dcm) / tot_ins
            l1_tcm = job['l1_icm'] + job['l1_dcm']
            job <<=  l1_tcm / job['tot_ins'] >> 'l1_miss_rate'

            # l1_miss_ratio = (l1_icm + l1_dcm) / (ld_ins + sr_ins)
            job <<= l1_tcm / mem_acc >> 'l1_miss_ratio'

            # l2_miss_rate = l2_tcm / tot_ins
            job <<= job['l2_tcm'] / job['tot_ins'] >> 'l2_miss_rate'

            # l2_miss_ratio = l2_tcm / mem_acc
            job <<= job['l2_tcm'] / mem_acc >> 'l2_miss_ratio'

            # l3_miss_rate = l3_tcm / tot_ins
            job <<= job['l3_tcm'] / job['tot_ins'] >> 'l3_miss_rate'

            # l3_miss_ratio = l3_tcm / mem_acc
            job <<= job['l3_tcm'] / mem_acc >> 'l3_miss_ratio'

            # l2_bandwidth = l2_tca * 64e-6
            job <<= job['l2_tca'] * 64e-6 >> 'l2_bw'

            # l3_bandwidth = (l3_tca) * 64e-6
            job <<= job['l3_tca'] * 64e-6 >> 'l3_bw'

            # floating_point
            job <<= job['fp_ops'] / job['tot_ins'] >> 'fp_rate'

            # branch
            job <<= job['br_ins'] / job['tot_ins'] >> 'branch_rate'

            # load
            job <<= job['ld_ins'] / job['tot_ins'] >> 'load_rate'

            # store
            job <<= job['sr_ins'] / job['tot_ins'] >> 'store_rate'

            return xfrm, job

        except Exception as e:
            a, b, c = sys.exc_info()
            print('derived_metrics: Error: '+str(e)+' '+str(c.tb_lineno))
            return None

    def papi_rank_stats(self, xfrm, job):
        try:
            """Return min/max/standard deviation/mean for papi derived metrics"""

            stats = DataSet()
            xfrm.push(job)
            events = job.series
            idx = events.index('rank')
            events = events[idx+1:]
            # compute the rank containing the minima for each event
            mins = DataSet()
            for name in events:
                xfrm.dup()
                xfrm.min([ name ], group_name='rank')
                xfrm.minrow(name+'_min')
                xfrm.top().rename('rank', name + '_min_rank')
                mins.append_series(xfrm.pop())

            # compute the rank containing the maxima for each event
            maxs = DataSet()
            for name in events:
                xfrm.dup()
                xfrm.max([ name ], group_name='rank')
                xfrm.maxrow(name+'_max')
                xfrm.top().rename('rank', name + '_max_rank')
                maxs.append_series(xfrm.pop())

            # compute the standard deviation
            xfrm.dup()
            xfrm.std(events)
            stats.append_series(xfrm.pop())

            # mean
            xfrm.mean(events)
            stats.append_series(xfrm.pop())

            return (events, mins, maxs, stats)
        except Exception as e:
            a, b, c = sys.exc_info()
            print('papi_rank_stats: Error: '+str(e)+' '+str(c.tb_lineno))
            return (None, None, None, None)
