from graf_analysis.grafanaFormatter import DataFormatter, RowIter
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

        self.result = { "type" : "table" }
        self.result['columns'] = [ { "text" : colName } for colName in self.data.series ]
        rows = []
        for row in RowIter(self.data):
            rows.append(row)
        self.result['rows'] = rows
        return [ self.result ]

    def fmt_dataframe(self):
        if self.data is None:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]

        plt_dict = { "type" : "table" }
        plt_dict['columns'] = [ { "text" : colName } for colName in self.data.series ]
        for series in self.data.columns:
            plt_dict = { "columns" : series }
            self.result.append(plt_dict)
        return self.result

    def fmt_builtins(self):
        if self.data is None:
            return [ { "columns" : [], "rows" : [], "type" : "table" } ]
        else:
            return self.data
