from builtins import next
import cython
from libc.stdint cimport *
from libc.stdlib cimport malloc, free
import numpy as np
from sosdb import Sos
from sosdb.DataSet import DataSet
from numsos import Csv
import datetime as dt
import time
import os
import struct
import sys

cdef class Default(object):
    DEF_ARRAY_LIMIT = 256
    cdef long start
    cdef long row_count
    cdef long limit
    cdef long array_limit
    cdef query
    cdef dataset

    def __init__(self, query, limit, start=0):
        cdef int typ
        cdef typ_str
        cdef col

        self.start = start
        self.row_count = start
        self.limit = limit
        self.array_limit = self.DEF_ARRAY_LIMIT
        self.query = query
        self.dataset = DataSet()
        for col in self.query.get_columns():
            typ = col.attr_type
            if typ == Sos.TYPE_TIMESTAMP:
                typ_str = 'datetime64[us]'
            elif typ == Sos.TYPE_STRUCT:
                typ_str = 'uint8'
            elif typ == Sos.TYPE_UINT64:
                typ_str = 'double'
            elif typ == Sos.TYPE_UINT32:
                typ_str = 'double'
            elif typ == Sos.TYPE_INT64:
                typ_str = 'double'
            elif typ == Sos.TYPE_INT32:
                typ_str = 'double'
            else:
                typ_str = Sos.sos_type_strs[typ].lower()
                typ_str = typ_str.replace('_array', '')

            if typ >= Sos.TYPE_IS_ARRAY:
                if typ == Sos.TYPE_STRING:
                    data = np.zeros([ self.limit ],
                                    dtype=np.dtype('|S{0}'.format(self.DEF_ARRAY_LIMIT)))
                else:
                    data = np.zeros([ self.limit, self.array_limit],
                                    dtype=np.dtype(typ_str))
            else:
                data = np.zeros([ self.limit ], dtype=np.dtype(typ_str))
            col.set_data(data)
            self.dataset.append_array(self.limit, [ col.col_name ], data)
        self.reset(start=start)

    def reset(self, start=0):
        if start:
            self.row_count = start
        else:
            self.row_count = self.start

    def input(self, row):
        cdef long typ

        for col in self.query.get_columns():
            typ = col.attr_type
            a = col.value
            array = col.get_data()
            if col.is_array or typ == Sos.TYPE_STRUCT:
                if typ != Sos.TYPE_STRING:
                    array[self.row_count,:len(a)] = a
                else:
                    array[self.row_count] = a
            elif typ == Sos.TYPE_TIMESTAMP:
                array[self.row_count] = (a[0] * 1000000) + a[1]
            else:
                array[self.row_count] = a
        self.row_count += 1
        if self.row_count == self.limit:
            return False
        return True

    def get_results(self):
        if self.row_count == 0:
            return None
        self.dataset.set_series_size(self.row_count)
        return self.dataset

class TableInputer(object):
    def __init__(self, query, limit, file=sys.stdout):
        self.row_count = 0
        self.limit = limit
        self.file = file

    @property
    def capacity(self):
        """Return the row capacity of the result"""
        return self.row_limit

    @property
    def count(self):
        """Return the number of rows in the result"""
        return self.row_count

    def input(self, query, reset=True):
        self.row_count = 0;
        if reset:
            if not query.descending:
                row = query.begin()
            else:
                row = query.end()
        else:
            if not query.descending:
                row = next(query)
            else:
                row = query.prev()
        if row:
            self.row_count = 1
        else:
            return True

        while self.row_count < self.limit:
            col_no = 0
            for col in query.get_columns():
                print(col.format(row[col_no]), end=' ', file=self.file)
                col_no += 1
            print("", file=self.file)
            if not query.descending:
                row = next(query)
            else:
                row = query.prev()
            if row:
                self.row_count += 1
            else:
                break

        if self.row_count == self.limit:
            return False
        return True

cdef class PandasDataFrame(object):
    DEF_ARRAY_LIMIT = 256
    cdef long start
    cdef long row_count
    cdef long limit
    cdef long array_limit
    cdef query
    cdef dataframe

    def __init__(self, query, limit, index=None, freq='1s', start=0):
        cdef int typ
        cdef typ_str
        cdef col

        import pandas as pd
        self.start = start
        self.row_count = start
        self.limit = limit
        self.array_limit = self.DEF_ARRAY_LIMIT
        self.query = query
        self.dataframe = pd.DataFrame()
        columns = []
        series = []
        for col in self.query.get_columns():
            typ = col.attr_type
            if typ == Sos.TYPE_TIMESTAMP:
                typ_str = 'datetime64[us]'
            elif typ == Sos.TYPE_STRUCT:
                typ_str = 'uint8'
            elif typ == Sos.TYPE_UINT64:
                typ_str = 'double'
            elif typ == Sos.TYPE_UINT32:
                typ_str = 'double'
            elif typ == Sos.TYPE_INT64:
                typ_str = 'double'
            elif typ == Sos.TYPE_INT32:
                typ_str = 'double'
            else:
                typ_str = Sos.sos_type_strs[typ].lower()
                typ_str = typ_str.replace('_array', '')

            if typ >= Sos.TYPE_IS_ARRAY:
                if typ == Sos.TYPE_STRING:
                    data = np.zeros([ self.limit ],
                                    dtype=np.dtype('|S{0}'.format(self.DEF_ARRAY_LIMIT)))
                else:
                    data = np.zeros([ self.limit, self.array_limit],
                                    dtype=np.dtype(typ_str))
            else:
                data = np.zeros([ self.limit ], dtype=np.dtype(typ_str))
            columns.append(col.col_name)
            if typ == Sos.TYPE_TIMESTAMP:
                df = pd.DataFrame(index=data, columns=col.col_name)
            else:
                df = pd.DataFrame(data, columns=col.col_name)
            series.append(df)
            #self.dataframe = self.dataframe.append(data=data, columns=col.col_name)
        self.dataframe = self.dataframe.merge(series)

    def reset(self, start=0):
        if start:
            self.row_count = start
        else:
            self.row_count = self.start

    def to_df(self):
        if self.row_count == 0:
            return None
        # self.dataset.set_series_size(self.row_count)
        return self.dataframe

    '''
    def input(self, row):
        cdef long typ

        for col in self.query.get_columns():
            typ = col.attr_type
            a = col.value
            array = col.get_data()
            if col.is_array or typ == Sos.TYPE_STRUCT:
                if typ != Sos.TYPE_STRING:
                    array[self.row_count,:len(a)] = a
                else:
                    array[self.row_count] = a
            elif typ == Sos.TYPE_TIMESTAMP:
                array[self.row_count] = (a[0] * 1000000) + a[1]
            else:
                array[self.row_count] = a
        self.row_count += 1
        if self.row_count == self.limit:
            return False
        return True

    def get_results(self):
        if self.row_count == 0:
            return None
        # self.dataset.set_series_size(self.row_count)
        return self.dataframe
    '''
