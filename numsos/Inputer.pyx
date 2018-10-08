from __future__ import print_function
from libc.stdint cimport *
from libc.stdlib cimport malloc, free
import numpy as np
from sosdb import Sos
from numsos import DataSet, Csv
import datetime as dt
import time
import os
import sys

cdef class Downsample(object):
    """This is the help"""
    cdef long bReset
    cdef long start
    cdef long row_count
    cdef long limit
    cdef long array_limit
    cdef long intervalUs
    cdef long last_time
    cdef sample_count
    cdef query
    cdef time_col
    cdef dataset

    DEF_ARRAY_LIMIT = 256
    def __init__(self, query, limit, intervalUs, timestamp='timestamp', start=0):
        self.bReset = 1
        self.start = start
        self.row_count = start
        self.limit = limit
        self.array_limit = self.DEF_ARRAY_LIMIT
        self.query = query
        self.intervalUs = intervalUs
        self.time_col = query.col_by_name(timestamp)
        self.last_time = 0
        cdef int typ

        if self.time_col == None:
            raise ValueError("The timestamp column '{0}' is not present "
                             "in the query.".foramt(timestamp))
        self.dataset = DataSet.DataSet()
        for col in self.query.get_columns():
            typ = col.attr_type
            if typ == Sos.TYPE_TIMESTAMP:
                typ_str = 'datetime64[us]'
            elif typ == Sos.TYPE_STRUCT:
                typ_str = 'uint8'
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
            self.dataset.append(DataSet.ArrayDataByIndex(self.limit, [ col.col_name ], data))
        self.sample_count = np.ndarray([ len(self.query.get_columns()) ])
        self.reset(start=start)

    def reset(self, start=0):
        self.bReset = 1
        if self.start:
            self.start = start
            self.row_count = start
        else:
            self.row_count = self.start
        self.sample_count[:] = 0.0

    def input(self, row):
        cdef int col_no
        cdef long t
        cdef int typ

        col_no = 0
        a = self.time_col.value
        t = (a[0] * 1000000) + a[1]
        if self.bReset:
            self.last_time = t
            self.bReset = 0
        else:
            if t - self.last_time >= self.intervalUs or t < self.last_time:
                self.row_count += 1
                self.last_time = t
                self.sample_count[:] = 0
                # print(self.row_count, self.limit)
        for col in self.query.get_columns():
            typ = col.attr_type
            a = col.value
            array = col.get_data()
            if col.is_array or typ == Sos.TYPE_STRUCT:
                if typ != Sos.TYPE_STRING:
                    array[self.row_count,:len(a)] = a
                else:
                    array[self.row_count] *= self.sample_count[col_no]
                    array[self.row_count] += a
                    array[self.row_count] /= (self.sample_count[col_no] + 1)
            elif typ == Sos.TYPE_TIMESTAMP:
                if self.sample_count[col_no] == 0:
                    array[self.row_count] = int(t - (t % self.intervalUs))
            else:
                array[self.row_count] *= self.sample_count[col_no]
                array[self.row_count] += a
                array[self.row_count] /= (self.sample_count[col_no] + 1.0)
            self.sample_count[col_no] += 1.0
            col_no += 1
        if self.row_count == self.limit:
            return False
        return True

    def get_results(self):
        self.bReset = 1       # ???
        if self.row_count == 0:
            return None
        self.dataset.set_series_size(self.row_count)
        return self.dataset

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
        self.dataset = DataSet.DataSet()
        for col in self.query.get_columns():
            typ = col.attr_type
            if typ == Sos.TYPE_TIMESTAMP:
                typ_str = 'datetime64[us]'
            elif typ == Sos.TYPE_STRUCT:
                typ_str = 'uint8'
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
            self.dataset.append(DataSet.ArrayDataByIndex(self.limit, [ col.col_name ], data))
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

