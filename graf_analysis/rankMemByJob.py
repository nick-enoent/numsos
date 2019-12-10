import datetime as dt
import time
from sosdb import Sos
from sosdb.DataSet import DataSet
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from grafanaAnalysis import Analysis
import numpy as np
import pandas as pd

class rankMemByJob(Analysis):
    def __init__(self, cont, start, end, schema='meminfo', maxDataPoints=4096):
        self.schema = schema
        self.cont = cont
        self.start = start
        self.end = end
        self.maxDataPoints = 4096
        self.src = SosDataSource()
        self.src.config(cont=cont)

    def get_data(self, metricNames, job_id, params):
        self.metrics = [ 'job_id', 'component_id', 'timestamp', 'MemTotal', 'MemFree' ]
        try:
            if params is None:
                res = self._job_summary(job_id)
                return res
            if 'threshold' in params:
                threshold = int(params.split('=')[1])
            else:
                threshold = 5
            if 'idle' in params:
                if threshold < 0:
                    res = self._get_idle_low_mem(abs(threshold))
                else:
                    res = self._get_idle_high_mem(threshold)
            else:
                if threshold < 0:
                    res = self._get_low_mem(abs(threshold))
                else:
                    res = self._get_high_mem(threshold)
            return res
        except:
            return None

    def _mem_used_ratio(self):
        self.xfrm = Transform(self.src, None)
        resp = self.xfrm.begin()
        while resp is not None:
            resp = self.xfrm.next()
            if resp is not None:
                self.xfrm.concat()
        try:
            data = self.xfrm.top()
        except:
            return None
        memUsedRatio = ((data['MemTotal'] - data['MemFree']) / data['MemTotal']) * 100 >> 'Mem_Used_Ratio'
        self.stdd = memUsedRatio.std()
        self.mean = memUsedRatio.mean()

        memUsedRatio <<= data['timestamp']
        memUsedRatio <<= data['job_id']
        memUsedRatio <<= data['component_id']
        self.xfrm.push(memUsedRatio)
        return memUsedRatio

    def _job_summary(self, job_id):
        ''' Get summarized information about jobs across components '''
        where_ = [ [ 'job_id', Sos.COND_EQ, job_id ] ]
        self.src.select(self.metrics,
                        from_ = [ self.schema ],
                        where = where_,
                        order_by = 'job_comp_time'
            )

        memUsedRatio = self._mem_used_ratio()
        if memUsedRatio is None:
            return None
        self.xfrm.push(memUsedRatio)
        res = self.xfrm.min([ 'Mem_Used_Ratio' ], group_name='job_id',
                            keep=['component_id'], xfrm_suffix='')
        self.xfrm.push(memUsedRatio)
        counts = [ len(res) ]
        _max = self.xfrm.max([ 'Mem_Used_Ratio' ], group_name='job_id',
                             keep=['component_id'], xfrm_suffix='')
        res = res.concat(_max)
        counts.append(len(_max))
        i = -2
        mem_used = []
        jid = []
        while i < 3:
            lim = self.mean[[0,0]] + float(i) * self.stdd[[0,0]]
            mem_used.append(lim)
            if i == 0:
                _count = []
            elif i < 0:
                _count = memUsedRatio < ('Mem_Used_Ratio', lim)
            else:
                _count = memUsedRatio > ('Mem_Used_Ratio', lim)

            counts.append(len(_count))
            del _count
            jid.append(job_id)
            i += 1
        _res = DataSet()
        _res.append_array(len(mem_used), 'Mem_Used_Ratio', mem_used)
        _res.append_array(5, 'job_id', jid)
        res = res.concat(_res)
        res.append_array(7, "Analysis", ["Min", "Max", "Stdd-2", "Stdd-1", "Mean", "Stdd+1", "Stdd+2" ])
        res.append_array(7, "Count", counts)
        return res

    def _get_high_mem(self, threshold):
        where_ = [ [ 'job_id', Sos.COND_GT, 1 ],
                   [ 'timestamp', Sos.COND_GE, self.start ] ]
        if self.end > 0:
            where_.append([ 'timestamp', Sos.COND_LE, self.end ])
        self.src.select(self.metrics,
                       from_ = [ self.schema ],
                       where = where_,
                       order_by = 'time_job_comp'
            )
        memUsedRatio = self._mem_used_ratio()
        if memUsedRatio is None:
            return None
        memUsedRatio = self.xfrm.max([ 'Mem_Used_Ratio' ], group_name='job_id',
                                     keep=['job_id', 'component_id'], xfrm_suffix='')
        self.xfrm.push(memUsedRatio)
        if memUsedRatio.get_series_size() > threshold:
            i = 0
            while i < threshold:
                _max = self.xfrm.maxrow('Mem_Used_Ratio')
                if i == 0:
                    res = _max
                else:
                    res = res.concat(_max)
                memUsedRatio = memUsedRatio < ('Mem_Used_Ratio', _max.array('Mem_Used_Ratio')[0])
                self.xfrm.push(memUsedRatio)
                i += 1
        else:
            res = self.xfrm.pop()
        return res

    def _get_low_mem(self, threshold):
        where_ = [ [ 'job_id', Sos.COND_GE, 1 ],
                   [ 'timestamp', Sos.COND_GE, self.start ] ]
        if self.end > 0:
            where_.append(['timestamp', Sos.COND_LE, self.end])
        self.src.select(self.metrics,
                        from_ = [ self.schema ],
                        where = where_,
                        order_by = 'time_job_comp'
            )
        memUsedRatio = self._mem_used_ratio()
        if memUsedRatio is None:
            return None
        memUsedRatio = self.xfrm.min([ 'Mem_Used_Ratio' ], group_name='job_id',
                                     keep=['job_id','component_id'], xfrm_suffix='')
        self.xfrm.push(memUsedRatio)
        if memUsedRatio.get_series_size() > threshold:
            i = 0
            while i < threshold:
                _min = self.xfrm.minrow('Mem_Used_Ratio')
                if i == 0:
                    res = _min
                else:
                    res = res.concat(_min)
                memUsedRatio = memUsedRatio > ('Mem_Used_Ratio', _min.array('Mem_Used_Ratio')[0])
                self.xfrm.push(memUsedRatio)
                i += 1
        else:
            res = self.xfrm.pop()
        return res

    def _get_idle_high_mem(self, threshold):
        ''' Get high mem threshold for idle nodes '''
        where_ = [ [ 'job_id', Sos.COND_EQ, 0 ],
                   [ 'timestamp', Sos.COND_GE, self.start ] ]
        if self.end > 0:
            where_.append([ 'timestamp', Sos.COND_LE, self.end ])
        self.src.select(self.metrics,
                   from_ = [ self.schema ],
                   where = where_,
                   order_by = 'time_comp'
            )
        memUsedRatio = self._mem_used_ratio()
        if memUsedRatio is None:
            return None
        if memUsedRatio.get_series_size() > threshold:
            ''' Check there are enough entries to slice '''
            i = 0
            while i < threshold:
                _max = self.xfrm.max([ 'Mem_Used_Ratio' ], group_name='component_id',
                                     keep=['timestamp', 'job_id', 'component_id'])
                if i == 0:
                    res = _max
                else:
                    res = res.concat(_max)
                memUsedRatio = memUsedRatio < ('Mem_Used_Ratio', _max.array('Mem_Used_Ratio_max')[0])
                self.xfrm.push(memUsedRatio)
                i += _max.get_series_size()
        else:
            ''' Return all components '''
            res = self.xfrm.pop()
        return res
        
    def _get_idle_low_mem(self, threshold):
        ''' Get low mem threshold for idle nodes '''
        where_ = [ [ 'job_id', Sos.COND_EQ, 0 ],
                   [ 'timestamp', Sos.COND_GE, self.start ] ]
        if self.end > 0:
            where_.append([ 'timestamp', Sos.COND_LE, self.end ])
        self.src.select(self.metrics,
                   from_ = [ self.schema ],
                   where = where_,
                   order_by = 'time_comp'
            )
        memUsedRatio = self._mem_used_ratio()
        if memUsedRatio is None:
            return None
        if memUsedRatio.get_series_size() > threshold:
            i = 0
            while i < threshold:
                _min = self.xfrm.min([ 'Mem_Used_Ratio' ], group_name='component_id',
                                     keep=['timestamp', 'job_id', 'component_id'])
                if i == 0:
                    res = _min
                else:
                    res = res.concat(_min)
                memUsedRatio = memUsedRatio > ('Mem_Used_Ratio', _min.array('Mem_Used_Ratio_min')[0])
                self.xfrm.push(memUsedRatio)
                i += _min.get_series_size()
        else:
            res = self.xfrm.pop()
        return res
