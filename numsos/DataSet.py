#!/usr/bin/env python
from __future__ import print_function
import numpy as np
import datetime as dt
import textwrap
import copy
import time
import os
import sys

class DataSet(object):
    """A convenience class for accessing datasource results

    The DataSet class wraps Numpy arrays and Python Lists that are
    returned by input(). The class provides a convenient way to access
    this data by series name.

    The DataSet stores numerical data as a numpy array and other data
    as a list. This allows the data to be conveniently accessed with a
    single interface while still supporting fast numpy analysis where
    possible.

    For example, consider the following code fragment:

        ds.config(path = '~/meminfo.csv')
        ds.select( [ 'timestamp', 'jobinfo.job_name',
                     'MemFree', 'MemAvailable' ],
                   from_ [ 'meminfo', 'jobinfo' ])
        result = ds.get_results()

    To get back an ndarray of the 'timestamp' series,
    do the following:

        tstamps = result['timestamp']

    The tstamps variable is a slice of the Numpy array that contains
    all of the timestamp data (i.e. the 'timestamp' series). No data
    is copied or moved.

    Similarly,

        job_names = result['jobinfo.job_name']

    The job_name variable is a Python list containing the
    'jobinfo.job_name' series. The job_name is maintained in a
    Python list character strings.

    Indexing is always [series][item]; the first diminesion is the
    series name (string) or number (int), and the second dimension
    is the datum index (int). Therefore data_set['timestamp'][0] is the
    first datum in the 'timestamp' series.
    """
    INDEX_ORDER  = 1            # Series are maintained in columns
    ATTR_ORDER   = 2            # Series are maintained in rows
    NUMERIC_DATA = 1            # ArrayData set
    LIST_DATA    = 2            # ListData set

    def __init__(self, order='index'):
        """Create an instance of a DataSet"""
        self.sets = []
        self.series_size = 0
        self.series_names = []
        self.set_with_series_name = {}
        self.set_col_map = []
        self.set_with_ser_idx = []
        if order == 'index':
            self.order = self.INDEX_ORDER
        else:
            self.order = self.ATTR_ORDER

    def copy(self, my_ser, my_offset, src_set, src_ser, src_offset, count):
        dst = self.set_with_series_name(my_ser)
        src = src_set.set_with_series_name(my_ser)
        dst.copy(my_ser, my_offset, src, src_ser, src_offset, count)

    def series_idx(self, name):
        return self.series_names.index(name)

    def rename(self, oldname, newname):
        """Rename a series

        Positional Parameters
        -- The series name
        -- The desired new series name
        """
        if newname in self.series_names:
            raise ValueError("The series name {0} already exists.".format(newname))
        if oldname not in self.series_names:
            raise ValueError("The series name {0} does not exist.".format(oldname))
        s = self.set_with_series_name[oldname]
        s.rename(oldname, newname)
        self.set_with_series_name[newname] = s
        del self.set_with_series_name[oldname]
        idx = self.series_names.index(oldname)
        self.series_names[idx] = newname

    def set_series_size(self, series_size):
        """Sets the size of the series present in the set

        This function is used to set how much data is present in
        each. Use the len() function to get the capacity (max-size) of
        a series.
        """
        self.series_size = series_size
        if type(self) == DataSet:
            for s in self.sets:
                s.set_series_size(series_size)

    def get_series_size(self):
        """Gets the size of the series

        This function is used to query how much data is actually
        present in a series. Use len() to get the capacity of a
        series.
        """
        return self.series_size

    def get_series_count(self):
        return len(self.series_names)

    def concat(self, aset, series_list=None):
        """Concatenate a set to the DataSet

        Add the new sets data to the end of this set

        Positional Parameters:
        -- The DataSet to get the data from

        """
        if series_list is None:
            series_list = aset.series

        # Build arrays of sets of the same type
        sets_with_type = {}
        for s in aset.sets:
            if type(s) in sets_with_type:
                sets_with_type[type(s)].append(s)
            else:
                sets_with_type[type(s)] = [ s ]

        # Build arrays of series with the same type
        series_with_type = {}
        for ser in series_list:
            s = self.set_with_series_name[ser]
            if type(s) in series_with_type:
                series_with_type[type(s)].append(ser)
            else:
                series_with_type[type(s)] = [ ser ]

        # Create the set list for the new DataSet
        newds = DataSet()
        row_count = self.series_size + aset.series_size
        for typ in series_with_type:
            series_names = series_with_type[typ]
            array = sets_with_type[typ][0].alloc(row_count, len(series_names))
            s = typ(row_count, series_names, array)
            newds.append(s)
        newds.set_series_size(row_count)

        # Copy the data from self
        for ser in series_list:
            dst = newds.set_with_series_name[ser]
            src = self.set_with_series_name[ser]
            dst.copy(ser, 0, src, ser, 0, src.series_size)

        # copy the data from aset
        for ser in series_list:
            dst = newds.set_with_series_name[ser]
            src = aset.set_with_series_name[ser]
            dst.copy(ser, self.series_size, src, ser, 0, aset.series_size)

        return newds

    def append(self, aset, series=None):
        """Append a set to the DataSet

        Add a new set at the end of the set list

        Positional Parameters:
        -- An ArrayData or ListData set to add to this DataSet

        """
        if aset.get_series_size() == 0:
            raise ValueError("Empty sets cannot be appended.")

        if series:
            add = False
            # check if any name in the input list is in this set
            for name in series:
                if name in aset.series:
                    add = True
                    break
        else:
            add = True
        if not add:
            return False

        if type(aset) == DataSet:
            if series:
                # Check that all series names are in at least one member set
                for ser in series:
                    valid = False
                    for s in aset.sets:
                        if ser in s.series:
                            valid = True
                            break
                    if not valid:
                        raise ValueError("The '{0}' series name is not present "
                                         "in this DataSet.".format(ser))
            add = False
            for s in aset.sets:
                if self.append(s, series=series):
                    add = True
            if not add:
                raise ValueError("No series in the series list was present in the set")
            return add

        if series is None:
            series_names = aset.series_names
        else:
            series_names = []
            for ser in series:
                if ser in aset.series:
                    series_names.append(ser)

        if self.get_series_size() == 0:
            self.set_series_size(aset.get_series_size())
        else:
            if aset.get_series_size() != self.get_series_size():
                raise ValueError("Appended sets must have compatible "
                                 "series size, {0} != {1}".format(
                                     self.get_series_size(),
                                     aset.get_series_size()))

        if aset not in self.sets:
            self.sets.append(aset)
        self.series_names += series_names
        for ser in series_names:
            if ser not in aset.series:
                # Ignore series names not present
                continue
            # given a series name return the set
            self.set_with_series_name[ser] = aset
            # given an index return the set
            self.set_with_ser_idx.append(aset)
            # given an index in this set, return the index in aset
            self.set_col_map.append(aset.series.index(ser))

        return True

    def __getitem__(self, idx):
        if type(idx) == str:
            s = self.set_with_series_name[idx]
            return s[idx]
        elif type(idx) == int:
            s = self.set_with_ser_idx[idx]
            return s[self.set_col_map[idx]]
        elif type(idx) == tuple:
            ser = idx[0]
            if type(ser) == str:
                s = self.set_with_series_name[ser]
                return s[idx]
            else:
                s = self.set_with_ser_idx[ser]
                return s[(self.set_col_map[idx[0]], idx[1])]
        else:
            raise ValueError("Unsupported index type {0}".format(type(idx)))

    def __setitem__(self, idx, value):
        if type(idx) == str:
            s = self.set_with_series_name[idx][idx]
            s[idx] = value
        elif type(idx) == int:
            s = self.set_with_ser_idx[idx]
            s[self.set_col_map[idx]] = value
        elif type(idx) == tuple:
            if type(idx[1]) == str:
                s = self.set_with_series_name[idx[0]]
                s[idx] = value
            else:
                s = self.set_with_ser_idx[idx[0]]
                s[(self.set_col_map[idx[0]], idx[1])] = value
        else:
            raise ValueError("Unsupported index type {0}".format(type(idx)))

    def __len__(self):
        return len(self.sets[0])

    def show(self, series=None, limit=None, width=16):
        wrap = []
        if series is None:
            series_names = self.series_names
        else:
            series_names = series
        for ser in series_names:
            name = ser.replace('.', '. ')
            wrap.append(textwrap.wrap(name, width))
        max_len = 0
        for col in wrap:
            if len(col) > max_len:
                max_len = len(col)
        for r in range(0, max_len):
            for col in wrap:
                skip = max_len - len(col)
                if r < skip:
                    label = ""
                else:
                    label = col[r - skip]
                print("{0:>{width}}".format(label, width=width), end=' ')
            print('')
        for ser in series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('')
        count = 0
        if limit is None:
            limit = self.get_series_size()
        else:
            limit = min(limit, self.get_series_size())
        for i in range(0, limit):
            for ser in series_names:
                v = self[ser, i]
                if type(v) == float:
                    print("{0:{width}}".format(v, width=width), end=' ')
                else:
                    print("{0:{width}}".format(v, width=width), end=' ')
            print('')
            count += 1
        for ser in series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('\n{0} results'.format(count))

    def new_set(self, data_type, ser_capacity, series_names, data=None):
        """Factor function for creating new ArraySet and DataSet classes

        Positional Parametes:
        -- The type of data in the set NUMPY_DATA or LIST_DATA
        -- Set contains these series
        -- The size of a data series

        Keyword Parameters:
        data  -- The data to be used. If None, data will be allocated

        Returns:
        A new DataSet of the appropriate type
        """
        if data_type == self.NUMERIC_DATA:
            if self.order == self.INDEX_ORDER:
                if data is None:
                    data = np.ndarray([ ser_capacity, len(series_names)])
                return ArrayDataByIndex(ser_capacity, series_names, data)
            else:
                if data is None:
                    data = np.ndarray([ ser_capacity, len(series_names)])
                return ArrayDataByColumn(ser_capacity, series_names, data)
        elif data_type == self.LIST_DATA:
            if self.order == self.INDEX_ORDER:
                return ListDataByIndex(ser_capacity, series_names)
            else:
                return ListDataByColumn(count, series_names)
        else:
            raise ValueError("{0} is an invalid data_type".format(data_type))

    def new(self, series_list=None):
        new_set = DataSet()
        if series_names is None:
            sets = []
            for s in self.sets:
                if type(s) == DataSet:
                    sets += s.sets
                else:
                    sets += [ s ]
            for s in sets:
                new_set.append(s.new())
        else:
            set_list = []
            set_series_names = {}
            for ser in series_names:
                s = self.set_with_series_name[ser]
                if s not in set_series_names:
                    set_list.append(s)
                    set_series_names[s] = []
                set_series_names[s].append(ser)
            for s in set_list:
                new_set.append(s.new(set_series_names[s]))
        return new_set

    @property
    def series_count(self):
        """Return the number of series in the DataSet"""
        return len(self.series_names)

    @property
    def series(self):
        """Return a copy of the series names

        This method returns a copy of the series names so that they
        can be manipulated by the caller without affecting the DataSet

        Use series_count() to obtain the number of series instead of
        len(series) to avoid unnecessarily copying data to obtain a
        series count.
        """
        return copy.copy(self.series_names)

class ArrayDataSet(DataSet):
    """Data set for ndarrays"""
    def __init__(self, series_size, series_names, array, order):
        """Create an instance of a DataSet

        This subclass of DataSet stores data as an ndarray.

        The DataSet interface works the same way regardless of the value
        of the 'order' keyword and will return the correct Numpy.ndarray
        or List slice.

        Positional Parameters:
        -- The capacity of the series in a DataSet
        -- An array of series names in the numpy array
        -- A numpy array

        Keyword Parameters:
        order -- INDEX_ORDER  Series are kept in 'columns'
              -- ATTR_ORDER   Series are kept in 'rows'

        Note that the 'order' keyword is an input to the constructor
        that instructs the class how the data stored in the array.
        Data is not recorganized based on this keyword, but rather
        specifies whether series names used with [] refer to rows or
        columns.
        """
        self.order = order
        self.series_size = series_size
        self.series_names = series_names
        self.array = array
        self.data_type = self.NUMERIC_DATA

    def alloc(self, row_count, ser_count):
        raise ValueError("Not implemented")

    def rename(self, oldname, newname):
        if newname in self.series_names:
            raise ValueError("The series name {0} already exists.".format(newname))
        if oldname not in self.series_names:
            raise ValueError("The series name {0} does not exist.".format(oldname))
        idx = self.series_names.index(oldname)
        self.series_names[idx] = newname

    def new(self, series_list=None):
        """Returns an instance that duplicates the meta-data of this set

        Positional Arguments:
        -- A list of series_names to be kept from this set in the result
        """
        raise ValueError("Not implemented!")

    def __getitem__(self, idx):
        raise ValueError("Not implemented!")

    def __setitem__(self, idx, value):
        raise ValueError("Not implemented!")

    def __len__(self):
        raise ValueError("Not implemented!")

    def show(self, width=16):
        for ser in self.series_names:
            print("{0:{width}}".format(ser, width=width), end=' ')
        print('')
        for ser in self.series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('')
        count = 0
        for row in self.array:
            for i in range(0, len(self.series_names)):
                v = row[i]
                if type(v) == float:
                    print("{0:{width}.2f}".format(row[i], width=width), end=' ')
                else:
                    print("{0:{width}}".format(row[i], width=width), end=' ')
            count += 1
            print('')
            if count > self.get_series_size():
                break
        for ser in self.series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('\n{0} results'.format(count))

    @property
    def series(self):
        return copy.copy(self.series_names)

class ArrayDataByIndex(ArrayDataSet):
    """Data set for ndarrays"""
    def __init__(self, row_count, series_names, array):
        """Create an ArrayData instance that stores data as an ndarray ordered by index"""
        ArrayDataSet.__init__(self, row_count, series_names, array, self.INDEX_ORDER)

    def alloc(self, row_count, series_count):
        """Allocate data suffient to store [row_count][series_count] items"""
        return np.ndarray([ row_count, series_count])

    def copy(self, my_ser, my_offset, src_set, src_ser, src_offset, count):
        """Copy series data from another set"""
        dst_idx = self.series_names.index(my_ser)
        src_idx = src_set.series_names.index(src_ser)
        self.array[my_offset:my_offset+count,dst_idx] \
            = src_set.array[src_offset:src_offset+count,src_idx]

    def new(self, series_list=None, series_size=None):
        """Returns an instance that duplicates the meta-data of this set

        Positional Arguments:

        -- A list of series_names to be included in the result. If not
           specified, all series_names in the set will be included.

        """

        if series_list is None:
            series_list = self.series_names
            ser_count = len(self.series_names)
        else:
            ser_count = 0
            for t in series_list:
                if t in self.series_names:
                    ser_count += 1
                else:
                    raise ValueError("The series name {0} is not present "
                                     "in this set.".format(t))
        if series_size is None:
            series_size = self.array.shape[0]
        shape = [ series_size, ser_count ]
        nda = np.ndarray(shape)

        return ArrayDataByIndex(self.series_size, series_list, nda)

    def concat(self, aset, series_list=None):
        if series_list is None:
            series_list = self.series

        series_size = self.get_series_size() + aset.get_series_size()
        res = self.new(series_size=series_size, series_list=series_list)

        dst_col = 0
        for ser in series_list:
            src_col = self.series_names.index(ser)
            res.array[0:self.get_series_size(), dst_col] = self.array[:self.get_series_size(), src_col]
            src_col = aset.series_names.index(ser)
            res.array[self.get_series_size():,dst_col] = aset.array[:aset.get_series_size(), src_col]
            dst_col += 1
        res.set_series_size(series_size)
        return res

    def __getitem__(self, idx):
        if type(idx) == int:
            if self.array.ndim > 1:
                return self.array[0:self.get_series_size():,idx]
            else:
                return self.array[0:self.get_series_size()]
        elif type(idx) == str:
            idx = self.series_names.index(idx)
            if self.array.ndim > 1:
                return self.array[0:self.get_series_size():,idx]
            else:
                return self.array[0:self.get_series_size()]
        elif type(idx) == tuple:
            ser = idx[0]
            row = idx[1]
            if type(ser) == str:
                ser = self.series_names.index(ser)
            if self.array.ndim > 1:
                return self.array[row][ser]
            else:
                return self.array[row]
        elif type(idx) == slice:
            return self.array[idx]
        else:
            raise ValueError("Invalid index type {0}".format(type(idx)))

    def __setitem__(self, idx, value):
        if type(idx) == int:
            self.array[0:self.get_series_size():,idx] = value
        elif type(idx) == str:
            idx = self.series_names.index(idx)
            self.array[0:self.get_series_size():,idx] = value
        elif type(idx) == tuple:
            ser = idx[0]
            row = idx[1]
            if type(ser) == str:
                ser = self.series_names.index(ser)
            self.array[row][ser] = value
        elif type(idx) == slice:
            self.array[idx] = value
        else:
            raise ValueError("Invalid index type {0}".format(type(idx)))

    def __len__(self):
        return len(self.array)

class ArrayDataByColumn(ArrayDataSet):
    """Data set for ndarrays"""
    def __init__(self, row_count, series_names, array, order='index'):
        raise ValueError("Unsupported")

    def alloc(self, row_count, series_count):
        """Allocate data suffient to store [row_count][series_count] items"""
        return np.ndarray([ series_count, row_count])

    def new(self, series_list=None):
        pass

    def __getitem__(self, idx):
        pass

    def __setitem__(self, idx, value):
        pass

    def __len__(self):
        pass

class ListDataSet(DataSet):
    """Data set for list results"""
    def __init__(self, capacity, series_names, alist, order):
        self.order = order
        self.series_size = capacity
        self.series_names = series_names
        self.alist = alist
        self.data_type = self.LIST_DATA

    def alloc(self, row_count, series_count):
        """Allocate data suffient to store [row_count][series_count] items"""
        raise ValueError("Unsupported")

    def new(self, series_list=None):
        """Returns an instance that duplicates the meta-data of this set

        The returned set will have the same series_names and initialized
        list data. This is necessary in order to allow for the indexing
        to work as expected when assigning new values to the set.

        """
        raise ValueError("Not implemented.")

    def __getitem__(self, idx):
        raise ValueError("Not implemented!")

    def __setitem__(self, idx, value):
        raise ValueError("Not implemented!")

    def __len__(self):
        raise ValueError("Not implemented!")

    def show(self, width=16):
        for ser in self.series_names:
            print("{0:{width}}".format(ser, width=width), end=' ')
        print('')
        for ser in self.series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('')
        count = 0
        for row in self.alist:
            for i in range(0, len(self.series_names)):
                v = row[i]
                if type(v) == float:
                    print("{0:{width}.2f}".format(row[i], width=width), end=' ')
                else:
                    print("{0:{width}}".format(row[i], width=width), end=' ')
            count += 1
            print('')
            if count > self.get_series_size():
                break
        for ser in self.series_names:
            print("{0:{width}}".format('-'.ljust(width, '-'), width=width), end=' ')
        print('\n{0} results'.format(count))

    @property
    def series(self):
        return copy.copy(self.series_names)

class ListDataByIndex(ListDataSet):
    """Data set for list results ordered by index"""

    def __init__(self, row_count, series_names, alist):
        ListDataSet.__init__(self, row_count, series_names, alist, self.INDEX_ORDER)

    def alloc(self, row_count, series_count):
        """Allocate data suffient to store [row_count][series_count] items"""
        alist = []
        if series_size is None:
            series_size = 0
        for row in range(0, series_count):
            new_row = []
            for col in range(0, row_count):
                new_row.append([])
            alist.append(new_row)
        return alist

    def copy(self, dst_ser, dst_offset, src_set, src_ser, src_offset, count):
        """Copy series data from another set"""
        pass

    def new(self, series_list=None, series_size=None):
        """Returns an instance that duplicates the meta-data of this set

        The returned set will have the same series_names and initialized
        list data. This necessary in order to allow for the indexing
        to work as expected when assigning new values to the set.

        """
        if series_names is None and series_size is None:
            c = copy.deepcopy(self)

        if series_names is None:
            series_names = self.series_names

        if series_size is None:
            series_size = self.series_size

        alist = self.alloc(series_size, len(series_names))
        return ListDataByIndex(len(alist), series_names, alist)

    def concat(self, aset, series_list=None):
        # TODO: series_list support
        series_size = self.get_series_size() + aset.get_series_size()
        res = self.new(series_size=series_size)

        for row in range(0, self.get_series_size()):
            for col in range(0, len(self.series_names)):
                res[row][col] = self.alist[row][col]

        for row in range(self.get_series_size(), series_size):
            for col in range(0, len(self.series_names)):
                res[row][col] = self.alist[row][col]

        res.set_series_size(series_size)
        return res

    def __getitem__(self, idx):
        if type(idx) == int:
            if len(self.series_names) > 1:
                return [ row[idx] for row in self.alist ]
            elif idx == 0:
                return self.alist
            else:
                raise ValueError("{0} is an invalid series number.".format(idx))
        elif type(idx) == str:
            idx = self.series_names.index(idx)
            return [ row[idx] for row in self.alist ]
        elif type(idx) == tuple:
            ser = idx[0]
            row = idx[1]
            if type(ser) == str:
                ser = self.series_names.index(ser)
            return self.alist[row][ser]
        elif slice == type(idx):
            return self.alist[idx]
        else:
            raise ValueError("Invalid index type {0}".format(type(idx)))

    def __setitem__(self, idx, value):
        if type(idx) == int:
            if len(self.series_names) > 1:
                # Value must be an array of the same dimension as self.alist
                row_no = 0
                for row in self.alist:
                    row[idx] = value[row_no]
                    row_no += 1
            elif idx == 0:
                self.alist = value
            else:
                raise ValueError("{0} is an invalid series number.".format(idx))
        elif type(idx) == str:
            idx = self.series_names.index(idx)
            return [ row[idx] for row in self.alist ]
        elif type(idx) == tuple:
            ser = idx[0]
            row = idx[1]
            if type(ser) == str:
                ser = self.series_names.index(ser)
            self.alist[row][ser] = value
        elif slice == type(idx):
            self.alist[idx] = value
        else:
            raise ValueError("Invalid index type {0}".format(type(idx)))

    def __len__(self):
        """Returns the max-number of series"""
        return len(self.alist)

class ListDataByColumn(ListDataSet):
    """Data set for list results"""

    def __init__(self, row_count, series_names):
        raise ValueError("Not implemented!")
        ListDataSet.__init__(self, row_count, series_names, self.ATTR_ORDER)

    def new(self):
        pass

    def __getitem__(self, idx):
        pass

    def __setitem__(self, idx, value):
        pass

    def __len__(self):
        pass
