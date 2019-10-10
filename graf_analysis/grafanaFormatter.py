from sosdb import Sos
from sosdb.DataSet import DataSet
import numpy as np
import copy

class RowIter:
    def __init__(self, dataSet):
        self.dset = dataSet
        self.limit = dataSet.get_series_size()
        self.row_no = 0

    def __iter__(self):
        return self

    def cvt(self, value):
        if type(value) == np.datetime64:
            return [ value.astype(np.int64) / 1000 ]
        return value

    def next(self):
        if self.row_no >= self.limit:
            raise StopIteration
        res = [ self.cvt(self.dset[[col, self.row_no]]) for col in range(0, self.dset.series_count) ]
        self.row_no += 1
        return res

class DataSetFormatter:
    def fmt_table(self, data):
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
        tbl_dict = { "type" : "table" }
        tbl_dict['columns'] = [ { "text" : colName } for colName in data.series ]
        rows = []
        for row in RowIter(data):
            rows.append(row)
        tbl_dict['rows'] = rows
        return [ tbl_dict ]

    def fmt_plot(self, data):
        return None
class DataFrameFormatter:
    def fmt_table(self, data):
        return 

    def fmt_plot(self, data):
        return None
