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

class lustrePeak(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.start = start
        self.end = end
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.job_metrics = [ 'jobid[ProducerName]', 'jobid[component_id]', 'jobid[job_id]',
                             'jobid[username]', 'jobid[uid]', 'jobid[timestamp]' ]

        self.where_ = []
        self.where_ = [ [ 'job_id', Sos.COND_GT, 1 ] ]
        if self.start > 0:
            self.where_.append(['timestamp', Sos.COND_GE, self.start])
        if self.end > 0:
            self.where_.append(['timestamp', Sos.COND_LE, self.end])

    def get_data(self, metrics, job_id=None, user_name=None, prdcr_name=None, params=None):
        self.user_name = user_name
        if params is not None:
            if 'threshold' in params:
               threshold = int(params.split('=')[1].split(',')[0])
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
        #res.show()
        del self.xfrm        
        return res

    def _sum_metrics(self, metrics):
        try:
            ''' Return tuple of (metric_per_second nd array, dataset) '''
            self.src.select([ 'job_id', 'component_id', 'timestamp' ] + metrics,
                       from_ = [ self.schema ],
                       where = self.where_,
                       order_by = 'job_time_comp'
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
            self.xfrm.dup()
            self.xfrm.for_each(series_list=['job_id'], xfrm_fn=self.xfrm.job_diff)
            return self.xfrm.sum_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e) + ' '+str(c.tb_lineno))
            return None, None

    def get_lustre_avg(self, metrics, threshold, meta=False):
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
            ret_prdcr = []
            i = 0
            jids = self.xfrm.job_ids
            while i < threshold:
                if len(sumbytes) < 1:
                    break
                index, val = max(enumerate(sumbytes), key=operator.itemgetter(1))
                where_ = [ [ 'job_id' , Sos.COND_EQ, jids[index] ] ]
                if self.user_name != None:
                    where_.append(['username', Sos.COND_EQ, self.user_name])
                self.src.select(self.job_metrics,
                                from_ = [ 'jobid' ],
                                where = where_,
                                order_by = 'job_comp_time'
                    )
                job = self.src.get_results()
                if job is None:
                    sumbytes = np.delete(sumbytes, index)
                    jids = np.delete(jids, index)
                    continue
                ret_bps.append(val)
                ret_jobs.append(job.array('job_id')[0])
                ret_user.append(job.array('username')[0])
                ret_prdcr.append(job.array('ProducerName')[0])

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
            res_.append_array(len(ret_user), 'username', ret_user)
            res_.append_array(len(ret_prdcr), 'ProducerName', ret_prdcr)
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
        try:
            self.diff(self.metrics, group_name='component_id', keep=['timestamp'],
                      xfrm_suffix='')
            sum_ = self.pop()
            if sum_.get_series_size() > 0:
                    times = sum_.array('timestamp').astype('int')
                    times = times/1000000
                    times = times.astype('int')
                    sum_ = sum_.append_array(len(times), 'times', times)
                    self.push(sum_)
                    self.sum(self.metrics, group_name='times', xfrm_suffix='')
                    sum_ = self.pop()
                    times = sum_.array('times').astype('int')
                    diff = []
                    for i in range(len(times)-1):
                        diff.append(times[i+1]-times[i])
                    rate = np.zeros(sum_.get_series_size()) 
                    for c in range(sum_.get_series_size()-1):
                            for m in self.metrics:
                                rate[c] += sum_.array(m)[c+1]/diff[c]
                    self.job_ids.append(values[0])
                    self.sum_.append(rate.max())
            else:
                    self.job_ids.append(values[0])
                    self.sum_.append(0.0)
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
            pass
