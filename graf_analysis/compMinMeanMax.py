from __future__ import absolute_import
from builtins import str
import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np
import time

class compMinMeanMax(Analysis):
    def __init__(self, cont, start, end, schema='meminfo', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.end = end
        self.maxDataPoints = maxDataPoints

    def get_data(self, metric, job_id, user_id=0, params=None):
        metric = metric[0]
        if job_id == 0:
            return [ { 'target' : 'Error: Please specify valid job_id', 'datapoints' : [] } ]
        # Get components with data during given time range
        self.src.select(['component_id'],
                   from_ = [ self.schema ],
                   where = [
                       [ 'job_id', Sos.COND_EQ, job_id ],
                       [ 'timestamp', Sos.COND_GE, self.start - 300 ],
                       [ 'timestamp', Sos.COND_LE, self.end + 300]
                   ],
                   order_by = 'job_time_comp'
            )
        comps = self.src.get_results(limit=self.maxDataPoints)
        if not comps:
            return [ { 'target' : 'Error: component_id not found for Job '+str(job_id),
                       'datapoints' : [] } ]
        else:
            compIds = np.unique(comps['component_id'].tolist())
        print(compIds)
        result = []
        datapoints = []
        time_range = self.end - self.start
        if time_range > 4096:
            bin_width = int(time_range // 200)
        else:
            bin_width = 1
        dfs = []
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
                       order_by = 'job_comp_time'
                )
            # default for now is dataframe - will update with dataset vs dataframe option
            res = self.src.get_df(limit=self.maxDataPoints, index='timestamp')
            if res is None:
                continue
            rs = res.resample(str(bin_width)+'S').fillna("backfill")
            dfs.append(rs)
        df = pd.concat(dfs, axis=1, ignore_index=True)
        res_ = DataSet()
        min_datapoints = df.min(axis=1, skipna=True)
        mean_datapoints = df.mean(axis=1, skipna=True)
        max_datapoints = df.max(axis=1, skipna=True)
        res_.append_array(len(min_datapoints), 'min_'+metric, min_datapoints)
        res_.append_array(len(mean_datapoints), 'mean_'+metric, mean_datapoints)
        res_.append_array(len(max_datapoints), 'max_'+metric, max_datapoints)
        res_.append_array(len(df.index), 'timestamp', df.index.values)
        return res_
