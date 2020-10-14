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

class lustrePeakAgg(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.start = start
        self.end = end
        self.schema = schema
        self.cont = cont
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.job_metrics = [ 'jobid[ProducerName]', 'jobid[component_id]', 'jobid[job_id]',
                             'jobid[username]', 'jobid[uid]', 'jobid[timestamp]' ]
        self.where_ = []
        if self.start > 0:
            self.where_.append(['timestamp', Sos.COND_GE, self.start])
        if self.end > 0:
            self.where_.append(['timestamp', Sos.COND_LE, self.end])

    def get_data(self, metrics, job_id=None, user_name=None, prdcr_name=None, params=None):
        self.user_name = user_name
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
        res = self.get_lustre_peak(metrics, threshold, meta=_meta)
        #res.show()
        del(self.xfrm)
        return res

    def get_lustre_peak(self, metrics, threshold, meta):
        if self.user_name != None:
                    self.where_.append(['username', Sos.COND_EQ, self.user_name])
        self.src.select(['username', 'job_id','component_id'],
                        from_ = [ 'jobid' ],
                        where = self.where_,
                        order_by = 'time_job_comp'
                    )
        self.xfrm = Xfrm(self.src, None)
        resp = self.xfrm.begin()
        if resp is None:
                    return None
        while resp is not None:
                resp = next(self.xfrm)
                if resp is not None:
                    self.xfrm.concat()
        res = self.xfrm.pop()
        self.xfrm.set_metrics(metrics)
        data = res.tolist()
        users = [item[0] for item in data]
        self.users = np.unique(users)
        self.users = np.delete(self.users, np.where(self.users == 'root'))
        jobs = []
        for i in self.users:
                dset = DataSet()
                jobs = np.unique([x[1] for x in data if x[0] == i])
                for i,job in enumerate(jobs):
                        jobm = self._get_job_metrics(metrics,job)
                        if jobm != None:
                                if dset.get_series_size() == 0:
                                        dset = jobm
                                else: 
                                        dset = dset.concat(jobm)
                self.xfrm.push(dset)
                self.xfrm.job_diff()
                resp = self.xfrm.pop()
        df = pd.DataFrame(list(zip(self.users, self.xfrm.sum_)),columns=['username', 'peak'])
        df = df.sort_values(by=['peak'], ascending=False)
        username = df.iloc[:threshold,0].to_numpy()
        peak = df.iloc[:threshold,1].to_numpy()
        ret = DataSet()
        ret.append_array(len(peak), 'peak', peak)
        ret.append_array(len(username), 'username', username)
        return ret

    def _get_job_metrics(self,metrics,job):
        try:
                src = SosDataSource()
                src.config(cont=self.cont)
                where_ = []
                if self.start > 0:
                    where_.append(['timestamp', Sos.COND_GE, self.start])
                if self.end > 0:
                    where_.append(['timestamp', Sos.COND_LE, self.end])
                where_.append(['job_id', Sos.COND_EQ, job])
                src.select([ 'job_id', 'component_id', 'timestamp' ] + metrics,
                       from_ = [ self.schema ],
                       where = where_,
                       order_by = 'job_time_comp'
                )
                xfrm = Xfrm(src, None)
                resp = xfrm.begin()
                if resp is None:
                        return None
                        while resp is not None:
                                resp = xfrm.next()
                                if resp is not None:
                                    xfrm.concat()
                res = xfrm.pop()
                return res

        except Exception as e:
                a, b, c = sys.exc_info()
                print(str(e) + ' '+str(c.tb_lineno))
                return None, None

class Xfrm(Transform):
    def set_metrics(self, metrics):
        self.metrics = metrics
        self.job_ids = []
        self.sum_ = []

    def job_diff(self):
        self.dup() 
        try:
            self.diff(self.metrics, group_name='component_id', keep=['timestamp'],
                      xfrm_suffix='')
            sum_ = self.pop()
            times = sum_.array('timestamp').astype('int')
            times = times/1000000/60
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
                        rate[c] += sum_.array(m)[c+1] / (diff[c]*60)
            self.sum_.append(rate.max())
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
            pass
