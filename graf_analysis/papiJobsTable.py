from graf_analysis.grafanaAnalysis import papiAnalysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import pandas as pd
import numpy as np
import sys
import time

class papiJobsTable(papiAnalysis):
    def get_data(self, metrics=None, job_id=None, user_id=0, params=None):
        try:
            where_ = [
                [ 'timestamp', Sos.COND_GE, self.start ],
                [ 'timestamp', Sos.COND_LE, self.end ],
                [ 'job_start', Sos.COND_GE, 1 ]
            ]
            self.metrics = [ 'job_id', 'job_size', 'uid', 'job_start',
                             'job_end', 'job_status', 'task_exit_status' ]
            self.src.select(self.metrics,
                       from_ = [ 'mt-slurm' ],
                       where = where_,
                       order_by = 'job_rank_time'
            )
            self.xfrm = Transform(self.src, None, limit=1000000)
            res = self.xfrm.begin()
            if not res:
                return None
            while res is not None:
                res = next(self.xfrm)
                if res is not None:
                    self.xfrm.concat()
            result = self.xfrm.pop()
            cols = [ { "text" : "job_id" },
                     { "text" : "CPU Dashboards" },
                     { "text" : "Cache Dashboards" },
                     { "text" : "job_size" },
                     { "text" : "user_id" },
                     { "text" : "job_status" },
                     { "text" : "job_start" },
                     { "text" : "job_end" },
                     { "text" : "task_exit_status" }
                   ]
            res_ = DataSet()
            jids = []
            cpu = []
            cache = []
            jsize = []
            uid = []
            jstatus = []
            jstart = []
            jend = []
            texit = []
            i = 0
            while i < result.get_series_size() - 1:
                if result.array('job_id')[i] in jids:
                    pass
                else:
                    jids.append(result.array('job_id')[i])
                    cpu.append('CPU Stats')
                    cache.append('Cache Stats')
                    jsize.append(result.array('job_size')[i])
                    uid.append(result.array('uid')[i])
                    jstatus.append(self.job_status_str[result.array('job_status')[i]])
                    jstart.append(result.array('job_start')[i] * 1000)
                    if result.array('job_end')[i] != 0:
                        jend.append(result.array('job_end')[i] * 1000)
                    else:
                        jend.append(time.time()*1000)
                    texit.append(result.array('task_exit_status')[i])
                i += 1
            res_.append_array(len(jids), 'job_id', jids)
            res_.append_array(len(cpu), 'CPU Stats', cpu)
            res_.append_array(len(cache), 'Cache Stats', cache)
            res_.append_array(len(jsize), 'job_size', jsize)
            res_.append_array(len(uid), 'user_id', uid)
            res_.append_array(len(jstatus), 'job_status', jstatus)
            res_.append_array(len(jstart), 'job_start', jstart)
            res_.append_array(len(jend), 'job_end', jend)
            res_.append_array(len(texit), 'task_exit_status', texit)
            return res_
        except Exception as e:
            a, b, c = sys.exc_info()
            print(str(e)+' '+str(c.tb_lineno))
            return None

