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

    def get_data(self, metrics, filters=[]):
        metric = metrics[0]
        select_clause = "select " + ",".join(metrics) + " from " + str(self.schema)
        self.start = float(self.start)
        self.end = float(self.end)
        where_clause = self.get_where(filters)
 
        try:
            self.query.select(f'{select_clause} {where_clause}')
            res = self.query.next()
            if res is None:
                return None
            df = res.copy(deep=True)
            while res is not None:
                ds = 15
                df['time_downsample'] = df['timestamp'].astype('int')/1e9
                df['time_downsample'] = df['time_downsample'].astype('int')%ds
                df = df[df['time_downsample'] == 0]
                df = df.drop(['time_downsample'],axis=1)
                df['timestamp'] = df['timestamp'].astype(int) / 1e9
                res = self.query.next()
                df = pd.concat([df, res])
            ret = pd.DataFrame(pd.unique(df['timestamp'].astype(int)*1000), columns=['timestamp'])
            ret['min'] = df.groupby(by=['timestamp'])[metric].min().reset_index()[metric]
            ret['mean'] = df.groupby(by=['timestamp'])[metric].mean().reset_index()[metric]
            ret['max'] = df.groupby(by=['timestamp'])[metric].max().reset_index()[metric]
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
