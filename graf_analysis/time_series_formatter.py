from .grafanaFormatter import DataFormatter
from sosdb.DataSet import DataSet
from sosdb import Sos
import numpy as np
import pandas as pd
import copy

class time_series_formatter(DataFormatter):
    def fmt_dataset(self):
        # timestamp is always last series
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]

        for series in self.data.series:
            if series == 'timestamp':
                continue
            ds = DataSet()
            ds.append_series(self.data, series_list=[series, 'timestamp'])
            plt_dict = { "target" : series }
            plt_dict['datapoints'] = ds.tolist()
            self.result.append(plt_dict)
            del ds
        return self.result

    def fmt_dataframe(self):
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]

        for series in self.data.columns:
            if series == 'timestamp':
                continue
            plt_dict = { "target" : series }
            plt_dict['datapoints'] = self.fmt_datapoints([series, 'timestamp'])
            self.result.append(plt_dict)
        return self.result

    def fmt_datapoints(self, series):
        ''' Format dataframe to output expected by grafana '''
        aSet = []
        for row_no in range(0, len(self.data)):
            aRow = []
            for col in series:
                v = self.data[col].values[row_no]
                typ = type(v)
                if typ.__module__ == 'builtins':
                    pass
                elif typ == np.ndarray or typ == np.string_:
                    v = str(v)
                elif typ == np.float32 or typ == np.float64:
                    v = float(v)
                elif typ == np.int64 or typ == np.uint64:
                    v = int(v)
                elif typ == np.int32 or typ == np.uint32:
                    v = int(v)
                elif typ == np.int16 or typ == np.uint16:
                    v = int(v)
                elif typ == np.datetime64:
                    # convert to milliseconds from microseconds
                    v = v.astype(np.int64) / int(1e6)
                else:
                    raise ValueError("Unrecognized numpy type {0}".format(typ))
                aRow.append(v)
            aSet.append(aRow)
        return aSet

    def fmt_builtins(self):
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]
        else:
            return self.data

