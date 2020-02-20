import os, sys, traceback, operator, time
import datetime as dt
from grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import time
import pandas as pd
import numpy as np

class lustreData(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.start = start
        self.end = end
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.job_metrics = [ 'mt-slurm[job_name]', 'mt-slurm[job_user]',
                             'mt-slurm[job_start]', 'mt-slurm[job_end]', 'mt-slurm[job_size]' ]
        self.where_ = []
        self.where_ = [ [ 'job_id', Sos.COND_GT, 1 ] ]
        if self.start > 0:
            self.where_.append(['timestamp', Sos.COND_GE, self.start])
        if self.end > 0:
            self.where_.append(['timestamp', Sos.COND_LE, self.end])

    def get_data(self, metrics, job_id=None, params=None):
        if params is not None:
            if 'threshold' in params:
               threshold = int(params.split('=')[1])
            else:
                threshold = 5
            if 'meta' in params:
                _meta = True
            else:
                _meta = False
        else:
            threshold = 5
            _meta = False
        res = self.get_lustre_avg(metrics, threshold, meta=_meta)
        return res

    def _sum_metrics(self, metrics):
        try:
            ''' Return tuple of (metric_per_second nd array, dataset) '''
            self.src.select([ 'job_id', 'component_id' ] + metrics,
                       from_ = [ self.schema ],
                       where = self.where_,
                       order_by = 'time_job_comp'
                )
            self.xfrm = Transform(self.src, None)
            resp = self.xfrm.begin()
            if resp is None:
                return None
            while resp is not None:
                resp = self.xfrm.next()
                if resp is not None:
                    self.xfrm.concat()
            
            self.xfrm.diff(metrics, group_name='component_id',
                           keep=['job_id'], xfrm_suffix='')
            sum_ = self.xfrm.max(metrics, group_name='job_id',
                                 keep=['job_id'], xfrm_suffix='')

            job_sums = np.zeros(sum_.get_series_size())
            for m in metrics:
                job_sums += sum_.array(m)
            return job_sums, sum_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e) + ' '+str(c.tb_lineno))
            return None, None

    def get_lustre_avg(self, metrics, threshold, meta=False):
        try:
            sumbytes, sum_ = self._sum_metrics(metrics)
            if sumbytes is None:
                return None
            ret_bps = []
            ret_jobs = []
            ret_name = []
            ret_start = []
            ret_end = []
            ret_user = []
            ret_state = []
            ret_size = []
            i = 0
            sumbytes = np.delete(sumbytes, 0)
            jids = sum_.array('job_id')[1:]
            while i < threshold:
                if len(sumbytes) < 1:
                    break
                index, val = max(enumerate(sumbytes), key=operator.itemgetter(1))
                self.src.select(self.job_metrics + ['job_id'],
                                from_ = [ 'mt-slurm' ],
                                where = [ [ 'job_id' , Sos.COND_EQ, jids[index] ] ],
                                order_by = 'job_rank_time'
                    )
                job = self.src.get_results()
                if job is None:
                    return None
                if job.array('job_end')[0] < 1:
                    job_end = time.time()
                    ret_end.append(job_end*1000)
                    ret_state.append("In process")
                else:
                    job_end = job.array('job_end')[0]
                    ret_end.append(job.array('job_end')[0]*1000)
                    ret_state.append("Completed")
                ret_bps.append(val / (job_end - job.array('job_start')[0]))
                ret_jobs.append(job.array('job_id')[0])
                ret_size.append(job.array('job_size')[0])
                ret_name.append(job.array('job_name')[0])
                ret_start.append(job.array('job_start')[0] * 1000)
                ret_user.append(job.array('job_user')[0])

                # remove job with highest bps from list of jobs
                sumbytes = np.delete(sumbytes, index)
                jids = np.delete(jids, index)
                i += 1
            res_ = DataSet()
            if not meta:
                res_.append_array(len(ret_bps), 'bps', ret_bps)
            else:
                res_.append_array(len(ret_bps), 'ios', ret_bps)
            res_.append_array(len(ret_jobs), 'job_id', ret_jobs)
            res_.append_array(len(ret_size), 'ranks', ret_size)
            res_.append_array(len(ret_name), 'job_name', ret_name)
            res_.append_array(len(ret_user), 'job_user', ret_user)
            res_.append_array(len(ret_start), 'job_start', ret_start)
            res_.append_array(len(ret_end), 'job_end', ret_end)
            res_.append_array(len(ret_state), 'job_state', ret_state)
            return res_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
            return None
