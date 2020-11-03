from graf_analysis.grafanaAnalysis import papiAnalysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np
import time
import sys
from IPython.core.debugger import set_trace

class papiTimeseries(papiAnalysis):
    def __init__(self, cont, start, end, schema=None, maxDataPoints=10000):
        super().__init__(cont, start, end, schema=schema, maxDataPoints=maxDataPoints)

    def get_data(self, metric, job_id=0, user_id=0, params=None):
        self.result = []
        metric = metric[0]
        if self.schema == 'kokkos_app':
            src.select([metric, 'start_time'],
                from_ = [ self.schema ],
                where = [
                    [ 'start_time', Sos.COND_GE, self.start ]
                ],
                order_by = 'job_id'
            )
            res = src.get_results()
            return res
        try:
            if not job_id:
                return { "target" : "Job Id required for papi_timeseries", datapoints : [] }
            xfrm, job = self.derived_metrics(job_id)
            if metric in self.event_name_map:
                metric = self.event_name_map[metric]
            datapoints = []
            i = 0
            while i < len(job.array(metric)):
                if i > 0:
                    if job.array('rank')[i-1] != job.array('rank')[i]:
                        self.result.append({"target" : '[Rank'+str(job.array('rank')[i-1])+']'+metric,
                                            "datapoints" : datapoints })
                        datapoints = []
                nda = np.array(job.array('timestamp'), dtype='double')
                dp = [ np.nan_to_num(job.array(metric)[i]), nda[i]/1000 ]
                datapoints.append(dp)
                i += 1
            self.result.append({"target" : '[Rank'+str(job.array('rank')[i-1])+']'+metric,
                                "datapoints" : datapoints })
            return self.result
        except Exception as e:
            a, b, c = sys.exc_info()
            return { "target" : str(e)+str(c.tb_lineno), "datapoints" : [] }
