import os, sys, traceback, operator, time
import datetime as dt
from grafanaAnalysis import Analysis
from grafanaFormatter import DataSetFormatter
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np

class compMinMeanMax(Analysis):
    def __init__(self, cont, start, end, schema='meminfo', maxDataPoints=4096):
        self.schema = schema
        self.src = SosDataSource()
        self.src.config(cont=cont)
        self.start = start
        self.send = end
        self.maxDataPoints = maxDataPoints

    def minMeanMax(self, metric, job_id):
        metric = metric[0]
        if job_id == 0:
            return [ { 'target' : 'Error: Please specify valid job_id', 'datapoints' : [] } ]
        # Get components with data during given time range
        self.src.select(['component_id'],
                   from_ = [ self.schema ],
                   where = [
                       [ 'timestamp', Sos.COND_GE, self.start ],
                       [ 'timestamp', Sos.COND_LE, self.send ],
                   ],
                   order_by = 'time_job_comp'
            )
        comps = self.src.get_results(limit=self.maxDataPoints)
        if not comps:
            return None
        else:
            compIds = np.unique(comps['component_id'].tolist())
        result = []
        # select job by job_id
        self.src.select(['job_start', 'job_end'],
                   from_ = [ 'mt-slurm' ],
                   where = [[ 'job_id', Sos.COND_EQ, job_id ]],
                   order_by = 'job_rank_time'
            )
        job = self.src.get_results()
        if job is None:
            return None
        job_start = job.array('job_start')[0]
        job_end = job.array('job_end')[0]
        datapoints = []
        for comp_id in compIds:
            where_ = [
                [ 'component_id', Sos.COND_EQ, comp_id ],
                [ 'job_id', Sos.COND_EQ, job_id ]
            ]
            self.src.select([ metric, 'timestamp' ],
                       from_ = [ self.schema ],
                       where = where_,
                       order_by = 'job_comp_time'
                )
            inp = None
            # default for now is dataframe - will update with dataset vs dataframe option
            res = self.src.get_df()
            if res is None:
                continue
            start_d = dt.datetime.utcfromtimestamp(job_start).strftime('%m/%d/%Y %H:%M:%S')
            end_d = dt.datetime.utcfromtimestamp(job_end).strftime('%m/%d/%Y %H:%M:%S')
            ts = pd.date_range(start=start_d, end=end_d, periods=len(res.values[0].flatten()))
            series = pd.DataFrame(res.values[0].flatten(), index=ts)
            rs = series.resample('S').ffill()
            datapoints.append(rs.values.flatten())
            tstamp = rs.index
        i = 0
        tstamps = []
        while i < len(tstamp):
            ts = pd.Timestamp(tstamp[i])
            ts = np.int_(ts.timestamp()*1000)
            tstamps.append(ts)
            i += 1
        min_ = DataSet()
        min_datapoints = np.min(datapoints, axis=0)
        min_.append_array(len(min_datapoints), 'min_'+metric, min_datapoints)
        min_.append_array(len(tstamps), 'timestamp', tstamps)
        result.append({ "target" : "min_"+str(metric), "datapoints" : min_.tolist() })

        mean = DataSet()
        mean_datapoints = np.mean(datapoints, axis=0)
        mean.append_array(len(mean_datapoints), 'mean_'+metric, mean_datapoints)
        mean.append_array(len(tstamps), 'timestamp', tstamps)
        result.append({"target" : "mean_"+str(metric), "datapoints" : mean.tolist() })

        max_ = DataSet()
        max_datapoints = np.max(datapoints, axis=0)
        max_.append_array(len(max_datapoints), 'max_'+metric, max_datapoints)
        max_.append_array(len(tstamps), 'timestamp', tstamps)
        result.append({"target" : "max_"+str(metric), "datapoints" : max_.tolist() })
        return result
