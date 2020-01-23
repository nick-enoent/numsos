import os, sys, traceback
import datetime as dt
from grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np

class compMinMeanMaxRate(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.end = end
        self.maxDataPoints = maxDataPoints

    def get_data(self, metric, job_id, params=None):
        metric = metric[0]
        if job_id == 0:
            return None
        # Get components with data during given time range
        self.src.select(['component_id'],
                   from_ = [ self.schema ],
                   where = [
                       [ 'job_id', Sos.COND_EQ, job_id ],
                       [ 'timestamp', Sos.COND_GE, self.start ],
                       [ 'timestamp', Sos.COND_LE, self.end ]
                   ],
                   order_by = 'time_job_comp'
            )
        comps = self.src.get_results(limit=4096)
        if not comps:
            return None
        else:
            compIds = np.unique(comps['component_id'].tolist())
        result = []
        # select job by job_id
        time_range = self.end - self.start
        if time_range > self.maxDataPoints:
            bin_width = int(time_range / 200)
        else:
            bin_width = 1
        datapoints = []
        for comp_id in compIds:
            where_ = [
                [ 'component_id', Sos.COND_EQ, comp_id ],
                [ 'job_id', Sos.COND_EQ, job_id ],
                [ 'timestamp', Sos.COND_GE, self.start ],
                [ 'timestamp', Sos.COND_LE, self.end ]
            ]
            self.src.select([ metric, 'timestamp' ],
                       from_ = [ self.schema ],
                       where = where_,
                       order_by = 'time_job_comp'
                )
            inp = None

            # default for now is dataframe - will update with dataset vs dataframe option
            res = self.src.get_df(limit=self.maxDataPoints)
            if res is None:
                continue
            ts = pd.date_range(start=pd.Timestamp(self.start, unit='s'), end=pd.Timestamp(self.end, unit='s'), periods=len(res.values[0].flatten()))
            series = pd.DataFrame(res.values[0].flatten(), index=ts)
            rs = series.resample(str(bin_width)+'S').fillna("backfill")
            dps = rs.values.flatten()
            dps = np.insert(dps, 0, 0)
            if len(dps) > 1:
                dps = np.diff(dps)
            datapoints.append(dps)
            tstamp = rs.index
        i = 0
        tstamps = []
        if len(tstamp) > 1:
            x = 1
        else:
            x = 0
        while i < len(tstamp[x:]):
            ts = pd.Timestamp(tstamp[i])
            ts = np.int_(ts.timestamp()*1000)
            tstamps.append(ts)
            i += 1
        res_ = DataSet()
        if len(datapoints) > 1:
            min_datapoints = np.min(datapoints, axis=0)
            mean_datapoints = np.mean(datapoints, axis=0)
            max_datapoints = np.max(datapoints, axis=0)
        else:
            min_datapoints = datapoints
            mean_datapoints = datapoints
            max_datapoints = datapoints
        res_.append_array(len(min_datapoints), 'min_'+metric, min_datapoints)
        res_.append_array(len(mean_datapoints), 'mean_'+metric, mean_datapoints)
        res_.append_array(len(max_datapoints), 'max_'+metric, max_datapoints)
        res_.append_array(len(tstamps), 'timestamp', tstamps)

        return res_
