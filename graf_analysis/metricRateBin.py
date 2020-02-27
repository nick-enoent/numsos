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

class metricRateBin(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.end = end
        self.maxDataPoints = maxDataPoints

    def get_data(self, metrics, job_id=0, params=''):
        result = []
        datapoints = []
        where_ = [ [ 'timestamp', Sos.COND_GE, self.start ],
                   [ 'timestamp', Sos.COND_LE, self.end ]
            ]
        if job_id > 0:
            where_.append(['job_id', Sos.COND_EQ, job_id])
        try:
            self.src.select(metrics + ['timestamp','component_id' ],
                       from_ = [ self.schema ],
                       where = where_,
                       order_by = 'time_comp_job'
                )
            inp = None

            # default for now is dataframe - will update with dataset vs dataframe option
            self.xfrm = Transform(self.src, None, limit=4096)
            resp = self.xfrm.begin()
            if resp is None:
                print('resp == None')
                return None

            while resp is not None:
                resp = self.xfrm.next()
                if resp is not None:
                    self.xfrm.concat()

            self.xfrm.diff(metrics, group_name="component_id",
                           keep=['timestamp'], xfrm_suffix='')

            data = self.xfrm.pop()
            hsum = None
            time_range = self.end - self.start
            bins = int(np.sqrt(time_range))
            if bins > 50:
                bins = 50
            for met_diff in metrics:
                os = data.array(met_diff)
                
                h = np.histogram(data.array('timestamp').astype('float'), bins=bins,
                                 weights=os, density=False)
                if hsum is None:
                    ts = h[1][:-1] / 1000
                    hsum = np.zeros(h[0].shape)
                hsum += h[0]
            res = DataSet()
            res.append_array(len(hsum), str(metrics), hsum)
            res.append_array(len(ts), 'timestamp', ts) 
            return res 
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))

