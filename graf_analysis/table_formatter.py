from .grafanaFormatter import DataFormatter, RowIter
from sosdb.DataSet import DataSet
from sosdb import Sos
import numpy as np
import pandas as pd
import copy

class table_formatter(DataFormatter):
    def fmt_dataset(self):
        # Format data from sosdb DataSet object
        if self.data is None:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]

        tbl_dict = { "type" : "table" }
        tbl_dict['columns'] = [ { "text" : colName } for colName in self.data.series ]
        rows = []
        for row in RowIter(self.data):
            rows.append(row)
        tbl_dict['rows'] = rows
        return [ tbl_dict ]

    def fmt_dataframe(self):
        if self.data is None:
            return [ { "target" : "", "datapoints" : [] } ]

        tbl_dict = { "type" : "table" }
        tbl_dict['columns'] = [ { "text" : colName } for colName in self.data.series ]
        for series in self.data.columns:
            plt_dict = { "columns" : series }
            self.result.append(plt_dict)
        return self.result
