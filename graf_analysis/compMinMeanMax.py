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
        super().__init__(cont, start, end, schema, maxDataPoints)

    def get_data(self, metrics, filters=[], params=None):
        select = self.select_clause(metrics)
        where_clause = self.get_where(filters)

        try:
            self.query.select(f'{select} {where_clause}')
            res = self.query.next()
            if res is None:
                return None
            df = res.copy(deep=True)
            while res is not None:
                res = self.query.next()
                df = pd.concat([df, res])
            ds = 15
            df['time_downsample'] = (df['timestamp'].astype(int)/int(1e9)).astype(int)
            df['time_downsample'] = df['time_downsample'].astype('int')%ds
            df = df[df['time_downsample'] == 0]
            df = df.drop(['time_downsample'],axis=1)
            df['timestamp'] = (df['timestamp'].astype(int)/int(1e9)).astype(int)
            df['timestamp'] = df['timestamp'] * 1000
            ret = pd.DataFrame(pd.unique(df['timestamp'].astype(int)), columns=['timestamp'])
            _min = df.groupby(by=['timestamp']).min().reset_index()
            _max = df.groupby(by=['timestamp']).max().reset_index()
            _mean = df.groupby(by=['timestamp']).mean().reset_index()
            for metric in metrics:
                ret[f'{metric}_min'] = _min[metric]
                ret[f'{metric}_mean'] = _mean[metric]
                ret[f'{metric}_max'] = _max[metric]
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
