import os, sys, traceback, operator, time
import datetime as dt
from grafanaAnalysis import Analysis
from grafanaFormatter import DataSetFormatter
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np

class lustreData(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client'):
        self.start = start
        self.end = end
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.job_metrics = [ 'mt-slurm[job_name]', 'mt-slurm[job_user]',
                             'mt-slurm[job_start]', 'mt-slurm[job_end]' ]
        self.f = DataSetFormatter()
        self.where_ = [ [ 'job_id', Sos.COND_GT, 1 ] ]
        if self.start > 0:
            self.where_.append(['timestamp', Sos.COND_GE, self.start])
        if self.end > 0:
            self.where_.append(['timestamp', Sos.COND_LE, self.end])

    def sum_metrics(self, metrics):
        try:
            ''' Return tuple of (metric_per_second nd array, dataset) '''
            self.src.select([ 'job_id' ] + metrics,
                       from_ = [ self.schema ],
                       where = self.where_,
                       order_by = 'time_job_comp'
                )
            self.xfrm = Transform(self.src, None)
            resp = self.xfrm.begin()
            while resp is not None:
                resp = self.xfrm.next()
                if resp is not None:
                    self.xfrm.concat()
            sum_ = self.xfrm.sum(metrics, group_name='job_id',
                                 keep=['job_id'], xfrm_suffix='')

            job_sums = np.zeros(sum_.get_series_size())
            for m in sum_.series[1:]:
                job_sums += sum_.array(m)
            return job_sums, sum_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e) + ' '+str(c.tb_lineno))
            return str(e) + ' '+str(c.tb_lineno)

    def get_lustre_avg(self, metrics, threshold, meta=False):
        sumbytes, sum_ = self.sum_metrics(metrics)
        ret_bps = []
        ret_jobs = []
        ret_name = []
        ret_start = []
        ret_end = []
        ret_user = []
        i = 0
        while i < threshold:
            index, val = max(enumerate(sumbytes), key=operator.itemgetter(1))
            jids = sum_.array('job_id')
            self.src.select(self.job_metrics,
                            from_ = [ 'mt-slurm' ],
                            where = [ [ 'job_id' , Sos.COND_EQ, sum_.array('job_id')[index] ] ],
                            order_by = 'job_rank_time'
                )
            job = self.src.get_results()
            ret_bps.append(val / (job.array('job_end')[0] - job.array('job_start')[0]))
            ret_jobs.append(sum_.array('job_id')[index])
            ret_name.append(job.array('job_name')[0])
            ret_start.append(job.array('job_start')[0]*1000)
            ret_end.append(job.array('job_end')[0]*1000)
            ret_user.append(job.array('job_user')[0])

            # remove job with highest bps from list of jobs
            sumbytes = np.delete(sumbytes, index)
            jids = np.delete(jids, index)
            i += 1
        res = DataSet()
        if not meta:
            res.append_array(len(ret_bps), 'bps', ret_bps)
        else:
            res.append_array(len(ret_bps), 'ios', ret_bps)
        res.append_array(len(ret_jobs), 'job_id', ret_jobs)
        res.append_array(len(ret_name), 'job_name', ret_name)
        res.append_array(len(ret_user), 'job_user', ret_user)
        res.append_array(len(ret_start), 'job_start', ret_start)
        res.append_array(len(ret_end), 'job_end', ret_end)
        res = self.f.fmt_table(res)
        return res
