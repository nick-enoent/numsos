from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from builtins import next
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

class metricRateBin(Analysis):
    def __init__(self, cont, start, end, schema='Lustre_Client', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.end = end
        self.mdp = maxDataPoints

    def get_data(self, metrics, job_id=0, user_id=0, params='bins=10'):
        self.bins = 10
        result = []
        datapoints = []
        time_range = self.end - self.start
        offset = time_range * .01
        if offset < 1:
            offset = 1
        where_ = [ [ 'timestamp', Sos.COND_GE, self.start - offset ],
                   [ 'timestamp', Sos.COND_LE, self.end + offset ]
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
            self.xfrm = Transform(self.src, None, limit=self.mdp)
            resp = self.xfrm.begin()
            if resp is None:
                print('resp == None')
                return None

            while resp is not None:
                resp = next(self.xfrm)
                if resp is not None:
                    self.xfrm.concat()
            self.xfrm.dup()
            data = self.xfrm.pop()

            self.xfrm.diff(metrics, group_name="component_id",
                           keep=['timestamp'], xfrm_suffix='')

            data = self.xfrm.pop()
            hsum = None
            data_time = (data.array('timestamp')[-1].astype('float') - data.array('timestamp')[0].astype('float'))
            data_time = data_time / 1000000
            if data_time < time_range:
                bins = int(data_time / time_range * 20)
                if bins < 2:
                    bins = 2
            else:
                bins = 20 
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

