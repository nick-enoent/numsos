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
    def __init__(self, data):
         self.result = []
         self.data = data
         self.fmt = type(self.data).__module__
         self.fmt_data = {
             'sosdb.DataSet' : self.fmt_dataset,
             'pandas.core.frame' : self.fmt_dataframe
         }

    def ret_json(self):
         return self.fmt_data[self.fmt]()

    def fmt_dataset(self):
        return None

    def fmt_dataframe(self):
        return None
