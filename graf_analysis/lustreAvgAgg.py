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

class lustreAvgAgg(Analysis):
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
        res = self.get_lustre_avg(metrics, threshold, meta=_meta)
        #res.show()
        return res

    def get_lustre_avg(self, metrics, threshold, meta):
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
        df = pd.DataFrame(list(zip(self.users, self.xfrm.sum_)),columns=['username', 'avg'])
        df = df.sort_values(by=['avg'], ascending=False)
        username = df.iloc[:threshold,0].to_numpy()
        avg = df.iloc[:threshold,1].to_numpy()
        ret = DataSet()
        ret.append_array(len(avg), 'avg', avg)
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

    #def job_diff(self, values):
    def job_diff(self):
        self.dup()
        try:
            #print(values)
            self.dup()
            sum_ = self.pop()
            self.dup()
            self.min(['timestamp'], xfrm_suffix='')
            min_ = self.pop()
            job_start = min_.array(0)[0].astype('int')
            self.dup()
            self.max(['timestamp'], xfrm_suffix='')
            max_ = self.pop()
            job_end = max_.array(0)[0].astype('int')
            job_total = (job_end - job_start)/1000000
            self.diff(self.metrics, group_name='component_id',
                      xfrm_suffix='')
            self.dup()
            sum_ = self.pop()
            self.sum(self.metrics, xfrm_suffix='')
            sum_ = self.pop()
            rate = 0
            for m in self.metrics:
                rate += sum_.array(m)[0]
            #self.job_ids.append(values[0])
            self.sum_.append(rate/job_total)
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
            pass
