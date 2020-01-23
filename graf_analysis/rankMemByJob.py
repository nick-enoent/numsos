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
        self.start = int(start)
        self.end = end
        self.maxDataPoints = 4096
        self.src = SosDataSource()
        self.src.config(cont=cont)

    def get_data(self, metricNames=None, job_id=None, params=None):
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

    def _get_job_times(self):
        self.xfrm.dup()
        self.xfrm.dup()
        self.xfrm.min([ 'timestamp' ], group_name='job_id',
                      xfrm_suffix='')
        job_start = self.xfrm.pop()
        self.xfrm.max([ 'timestamp' ], group_name='job_id', xfrm_suffix='')
        job_end = self.xfrm.pop()
        self.xfrm.pop()
        job_times = job_start['timestamp'] >> 'job_start'
        job_times <<= job_end['timestamp'] >> 'job_end'
        return job_times

    def _job_summary(self, job_id):
        ''' Get summarized information about jobs across components '''
        where_ = [ [ 'job_id', Sos.COND_EQ, job_id ] ]
        # order_by = 'job_comp_time'
        self.src.select(self.metrics,
                        from_ = [ self.schema ],
                        where = where_,
                        order_by = 'time_job_comp'
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
        self.xfrm.dup()
        job_times = self._get_job_times()
        keep_ = [ 'job_id', 'component_id' ]
        self.xfrm.max([ 'Mem_Used_Ratio' ], group_name='job_id',
                      keep=keep_, xfrm_suffix='')
        memUsedRatio = self.xfrm.pop()
        memUsedRatio <<= job_times['job_start']
        memUsedRatio <<= job_times['job_end']
        memUsedRatio.array('Mem_Used_Ratio').sort(0)
        if memUsedRatio.get_series_size() > threshold:
            _max = memUsedRatio.array('Mem_Used_Ratio')[memUsedRatio.get_series_size() - (threshold)]
            memUsedRatio = memUsedRatio > ('Mem_Used_Ratio', _max)
        return memUsedRatio

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
        self.xfrm.dup()
        job_times = self._get_job_times()
        keep_ = [ 'job_id', 'component_id' ]
        self.xfrm.min([ 'Mem_Used_Ratio' ], group_name='job_id',
                                     keep=keep_, xfrm_suffix='')
        memUsedRatio = self.xfrm.pop()
        memUsedRatio <<= job_times['job_start']
        memUsedRatio <<= job_times['job_end']
        memUsedRatio.array('Mem_Used_Ratio').sort(0)
        if memUsedRatio.get_series_size() > threshold:
            _min = memUsedRatio.array('Mem_Used_Ratio')[threshold]
            memUsedRatio = memUsedRatio < ('Mem_Used_Ratio', _min)
            
        return memUsedRatio

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
