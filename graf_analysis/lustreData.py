import os, sys, traceback, operator, time
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
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
        self.job_metrics = [ 'mt-slurm[job_name]', 'mt-slurm[job_user]', 'mt-slurm[job_id]',
                             'mt-slurm[job_start]', 'mt-slurm[job_end]', 'mt-slurm[job_size]' ]
        self.where_ = []
        self.where_ = [ [ 'job_id', Sos.COND_GT, 1 ] ]
        if self.start > 0:
            self.where_.append(['timestamp', Sos.COND_GE, self.start])
        if self.end > 0:
            self.where_.append(['timestamp', Sos.COND_LE, self.end])

    def get_data(self, metrics, job_id=None, user_id=0, params=None):
        self.user_id = user_id
        self.parse_params(params)
        res = self.get_lustre_avg(metrics)
        return res

    def _sum_metrics(self, metrics):
        try:
            ''' Return tuple of (metric_per_second nd array, dataset) '''
            self.src.select([ 'job_id', 'component_id' ] + metrics,
                       from_ = [ self.schema ],
                       where = self.where_,
                       order_by = 'time_job_comp'
                )
            self.xfrm = Xfrm(self.src, None)
            # set metrics in Xfrm class
            self.xfrm.set_metrics(metrics)

            resp = self.xfrm.begin()
            if resp is None:
                return None
            while resp is not None:
                resp = next(self.xfrm)
                if resp is not None:
                    self.xfrm.concat()

            self.xfrm.for_each(series_list=['job_id'], xfrm_fn=self.xfrm.job_diff)
            return self.xfrm.sum_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e) + ' '+str(c.tb_lineno))
            return None, None

    def get_lustre_avg(self, metrics):
        try:
            sumbytes = self._sum_metrics(metrics)
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
            jids = self.xfrm.job_ids
            res = []
            while i < self.threshold:
                if len(sumbytes) < 1:
                    break
                index, val = max(enumerate(sumbytes), key=operator.itemgetter(1))
                where_ = [ [ 'job_id' , Sos.COND_EQ, jids[index] ] ]
                if self.user_id != 0:
                    where_.append(['uid', Sos.COND_EQ, self.user_id])
                self.src.select(self.job_metrics,
                                from_ = [ 'mt-slurm' ],
                                where = where_,
                                order_by = 'job_rank_time'
                    )
                job = self.src.get_results()
                res.append(job)
                if job is None:
                    sumbytes = np.delete(sumbytes, index)
                    jids = np.delete(jids, index)
                    continue
                job_start = np.min(job.array('job_start'))
                if job.array('job_end')[0] < 1:
                    job_end = time.time()
                    ret_end.append(job_end*1000)
                    ret_state.append("In process")
                else:
                    job_end = np.max(job.array('job_end'))
                    ret_end.append(job_end*1000)
                    ret_state.append("Completed")
                ret_bps.append(val / (job_end - job_start))
                ret_jobs.append(job.array('job_id')[0])
                ret_size.append(job.array('job_size')[0])
                ret_name.append(job.array('job_name')[0].decode())
                ret_start.append(job_start * 1000)
                ret_user.append(job.array('job_user')[0].decode())

                # remove job with highest bps from list of jobs
                sumbytes = np.delete(sumbytes, index)
                jids = np.delete(jids, index)
                i += 1
            res_ = DataSet()
            if not self._meta:
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

class Xfrm(Transform):
    def set_metrics(self, metrics):
        self.metrics = metrics
        self.job_ids = []
        self.sum_ = []

    def job_diff(self, values):
        self.dup()
        try:
            self.diff(self.metrics, group_name='component_id',
                      xfrm_suffix='')
            #self.mean(self.metrics, xfrm_suffix='')
            self.sum(self.metrics, xfrm_suffix='')
            sum_ = self.pop()
            rate = 0
            for m in self.metrics:
                rate += sum_.array(m)[0]
            self.job_ids.append(values[0])
            self.sum_.append(rate)

        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
