from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from graf_analysis.grafanaAnalysis import papiAnalysis
import numpy as np
import pandas as pd

"""Return mean papi metrics across ranks for a given job_id"""
class papiMeanJobMetrics(papiAnalysis):
    def get_data(self, metrics, job_id, user_id=0, params=None):
        if not job_id:
            return [ { "columns" : [{"text":"No Job Id specified"}], "rows" : [], "type" : "table" } ]
        xfrm, job = self.derived_metrics(job_id)
        if not job:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]
        xfrm.push(job)
        idx = job.series.index('tot_ins')
        series = job.series[idx:]
        xfrm.mean(series, group_name='rank', keep=job.series[0:idx-1], xfrm_suffix='')
        job = xfrm.pop()
        result = {}
        rows = []
        columns = []
        series_names = [ 'timestamp', 'job_id', 'component_id',
                         'rank',
                         'cpi', 'uopi',
                         'l1_miss_rate', 'l1_miss_ratio',
                         'l2_miss_rate', 'l2_miss_ratio',
                         'l3_miss_rate', 'l3_miss_ratio',
                         'fp_rate', 'branch_rate',
                         'load_rate', 'store_rate' ]
        idx = series_names.index('timestamp')
        del series_names[idx]
        idx = series_names.index('job_id')
        del series_names[idx]
        idx = series_names.index('component_id')
        del series_names[idx]
        res_ = DataSet()
        for series in series_names:
            array = job.array(series)
            res_.append_array(len(array), series, np.nan_to_num(array))
        return res_
