from graf_analysis.grafanaAnalysis import papiAnalysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import sys
import pandas as pd
import numpy as np

""" Returns a statistical summary of a given papi Job (by job_id)
    for the given metric in Table format """
class papiJobStatTable(papiAnalysis):
    def get_data(self, metrics, job_id, user_id=0, params=None):
        try:
            result = {}
            columns = [
                { "text" : "Metric" },
                { "text" : "Min" },
                { "text" : "Rank w/Min" },
                { "text" : "Max" },
                { "text" : "Rank w/Max" },
                { "text" : "Mean" },
                { "text" : "Standard Deviation" }
            ]
            result['columns'] = columns
            if not job_id:
                print('no job_id')
                return None
            xfrm, job = self.derived_metrics(job_id)
            events, mins, maxs, stats = self.papi_rank_stats(xfrm, job)
            res_ = DataSet()
            mins_ = []
            minranks = []
            maxs_ = []
            mxranks = []
            means = []
            stds_ = []
            for name in events:
                mins_.append(np.nan_to_num(mins.array(name+'_min')[0]))
                minranks.append(np.nan_to_num(mins.array(name+'_min_rank')[0]))
                maxs_.append(np.nan_to_num(maxs.array(name+'_max')[0]))
                mxranks.append(np.nan_to_num(maxs.array(name+'_max_rank')[0]))
                means.append(np.nan_to_num(stats.array(name+'_mean')[0]))
                stds_.append(np.nan_to_num(stats.array(name+'_std')[0]))
            res_.append_array(len(events), 'Metric', events)
            res_.append_array(len(mins_), 'Min Value', mins_)
            res_.append_array(len(minranks), 'Rank', mxranks)
            res_.append_array(len(maxs_), 'Max Value', maxs_)
            res_.append_array(len(means), 'Mean Value', means)
            res_.append_array(len(stds_), 'Stdd', stds_)
            return res_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e) + ' '+str(c.tb_lineno))
            return None
