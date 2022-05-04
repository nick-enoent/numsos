import os, sys, traceback
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from sosdb import Sos
import pandas as pd
import numpy as np

class metricRateBin(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        super().__init__(cont, start, end, schema, maxDataPoints)

    def get_data(self, metrics, filters=[], params='bins=10'):
        select = self.select_clause(metrics)
        where_clause = self.get_where(filters)
        self.bins = 10
        result = []
        datapoints = []
        time_range = self.end - self.start
        try:
            self.query.select(f'{select} {where_clause}')

            # default for now is dataframe - will update with dataset vs dataframe option
            df = self.query.next()
            if df is None:
                return None
            df = df[~df.index.duplicated(keep='first')]
            tstamps = df['timestamp'].astype('int') / 1e6
            df = df.drop('timestamp', axis=1)
            data_time = tstamps[-1] - tstamps[0]
            if data_time < time_range:
                bins = int(data_time / time_range * 20)
                if bins < 2:
                    bins = 2
            else:
                bins = 20
            dff = df.groupby(['component_id']).diff().fillna(0)
            hsum = None
            metric_str = ''
            for metric in metrics:
                if len(metric_str) > 0:
                    metric_str += ', '
                metric_str += metric
                os = dff[metric]
                h = np.histogram(tstamps, bins=bins,
                                 weights=os, density=False)
                if hsum is None:
                    ts = h[1][:-1]
                    hsum = np.zeros(h[0].shape)
                hsum += h[0]
            ret = pd.DataFrame(ts, columns=['timestamp'])
            ret.insert(0, f'{metric_str} Rate', hsum, True)
            
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))

