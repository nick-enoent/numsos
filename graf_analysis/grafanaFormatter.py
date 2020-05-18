from __future__ import division
from builtins import range
from builtins import object
from sosdb import Sos
from sosdb.DataSet import DataSet
import numpy as np
import copy

class RowIter(object):
    def __init__(self, dataSet):
        self.dset = dataSet
        self.limit = dataSet.get_series_size()
        self.row_no = 0

    def __iter__(self):
        return self

    def cvt(self, value):
        if type(value) == np.datetime64:
            return [ value.astype(np.int64) // 1000 ]
        return value

    def __next__(self):
        if self.row_no >= self.limit:
            raise StopIteration
        res = [ self.cvt(self.dset[[col, self.row_no]]) for col in range(0, self.dset.series_count) ]
        self.row_no += 1
        return res

class DataSetFormatter(object):
    def __init__(self, data, fmt):
         self.result = []
         self.data = data
         self.fmt = fmt
         self.fmt_data = {
             'table' : self.fmt_table,
             'time_series' : self.fmt_plot
         }

    def ret_json(self):
         return self.fmt_data[self.fmt]()
        
    def fmt_table(self):
        """
        [
        {
        "rows": [[[10500116.0], [ [6597484.0], [6594808.0]],
                 [[234562], [ [2345234], [23452672]], ...],
        "type": "table",
        "columns": [
                    {"text": "col-A"},
                    {"text" : "col-B"},
                    . . .
                   ]
        }
        ]
        """
        if self.data is None:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]

        tbl_dict = { "type" : "table" }
        tbl_dict['columns'] = [ { "text" : colName } for colName in self.data.series ]
        rows = []
        for row in RowIter(self.data):
            rows.append(row)
        tbl_dict['rows'] = rows
        return [ tbl_dict ]

    def fmt_plot(self):
        # timestamp is always last series
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]

        for series in self.data.series[:-1]:
            ds = DataSet()
            ds.append_series(self.data, series_list=[series, 'timestamp'])
            plt_dict = { "target" : series }
            plt_dict['datapoints'] = ds.tolist()
            self.result.append(plt_dict)
            del ds
        return self.result

class DataFrameFormatter(object):
    def fmt_table(self, data):
        return None

    def fmt_plot(self, data):
        return None
