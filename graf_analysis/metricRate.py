import sys
from graf_analysis.grafanaAnalysis import Analysis
from sosdb import Sos
import pandas as pd

class metricRate(Analysis):
    def __init__(self, cont, start, end, schema, maxDataPoints=10000):
        super().__init__(cont, start, end, schema, maxDataPoints)

    def get_data(self, metrics, filters=[], params=None):
        select = self.select_clause(metrics)
        where_clause = self.get_where(filters)
        self.query.select(f'{select} {where_clause}')
        res = self.query.next()
        if res is None:
            return None
        try:
            res = res[~res.index.duplicated(keep='first')]
            ret = pd.DataFrame(res['timestamp'].astype('int') / 1e6 , columns=['timestamp'])
            res = res.drop('timestamp', axis=1)
            res = res.groupby(['component_id'], as_index=False).diff()
            ret.insert(1, f'Active Rate', res['Active'], True)
            return ret
        except Exception as e:
            a, b, c = sys.exc_info()
            print(f'{str(e)} {str(c.tb_lineno)}')
            return None
