from __future__ import division
from builtins import range
from builtins import object
from sosdb import Sos
from sosdb.DataSet import DataSet
import numpy as np
import pandas as pd
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

class DataFormatter(object):
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
        '''
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
        '''
        return None

    def fmt_plot(self):
        return None

class DataSetFormatter(DataFormatter): 
    def fmt_table(self):
        ''' Format data from sosdb DataSet object '''
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

class DataFrameFormatter(DataFormatter):
    ''' Format Data from a pandas DataFrame object '''
    def fmt_table(self, data):
        ''' Format data from sosdb DataSet object '''
        if self.data is None:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]

        tbl_dict = { "type" : "table" }
        tbl_dict['columns'] = [ { "text" : colName } for colName in self.data.columns ]
        rows = []
        for row in self.data.values
            rows.append(list(row))
        tbl_dict['rows'] = rows
        return [ tbl_dict ]

    def fmt_plot(self):
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]

        for series in self.data.columns:
            if series == 'timestamp':
                continue
            plt_dict = { "target" : series }
            plt_dict['datapoints'] = self.fmt_df([series, 'timestamp']) 
            self.result.append(plt_dict)
        return self.result

    def fmt_df(self, series):
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
                    v = v.astype(np.int64) // int(1e6)
                else:
                    raise ValueError("Unrecognized numpy type {0}".format(typ))
                aRow.append(v)
            aSet.append(aRow)
        return aSet
