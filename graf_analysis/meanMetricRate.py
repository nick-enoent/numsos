import os, sys, traceback
import datetime as dt
from grafanaAnalysis import Analysis
from grafanaFormatter import DataSetFormatter
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np

class allMinMeanMaxRate(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.end = end
        self.maxDataPoints = maxDataPoints

    def get_data(self, metrics, job_id=0, params=None):
        result = []
        datapoints = []
        where_ = [ [ 'timestamp', Sos.COND_GE, self.start ],
                   [ 'timestamp', Sos.COND_LE, self.end ]
            ]
        self.src.select(metrics + ['timestamp' ],
                   from_ = [ self.schema ],
                   where = where_,
                   order_by = 'time_job_comp'
            )
        inp = None

        # default for now is dataframe - will update with dataset vs dataframe option
        res = self.src.get_df()
        if res is None:
            return None
        mets = res.drop(res.tail(1).index)
        mets = mets.mean()
        time_range = self.end - self.start
        if time_range > 4096:
            bin_width = int(time_range / 200)
        else:
            bin_width = 1
        start_d = dt.datetime.utcfromtimestamp(self.start).strftime('%m/%d/%Y %H:%M:%S')
        end_d = dt.datetime.utcfromtimestamp(self.end).strftime('%m/%d/%Y %H:%M:%S')
        ts = pd.date_range(start=start_d, end=end_d, periods=len(mets.values))
        series = pd.DataFrame(mets.values, index=ts, dtype=float)
        rs = series.resample(str(bin_width)+'S').mean()
        dps = rs.values.flatten()
        if len(dps) > 1:
            dps = np.diff(dps)
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
        res_.append_array(len(dps), str(metrics)+" Rate", dps)
        res_.append_array(len(tstamps), 'timestamp', tstamps)

        return res_
