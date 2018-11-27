from __future__ import print_function
import numpy as np
from sosdb import Sos
from sosdb.DataSet import DataSet
from numsos import Csv
from numsos import Inputer
import datetime as dt
import time
import os
import sys

class TableInputer(object):
    def __init__(self, query, limit, file=sys.stdout):
        self.row_count = 0
        self.limit = limit
        self.query = query
        self.file = file

    def input(self, row):
        try:
            for col in self.query.get_columns():
                print(col, end=' ', file=self.file)
            print("", file=self.file)
        except StopIteration:
            return False
        self.row_count += 1
        if self.row_count == self.limit:
            return False
        return True

def group_cols_by_type(cols):
    """Group together the columns in the record by their type"""
    groups = {}
    for typ in Sos.sos_type_strs:
        if typ == Sos.TYPE_TIMESTAMP or type == Sos.TYPE_STRUCT:
            continue
        for col in cols:
            col_type = col.attr_type
            if col_type == Sos.TYPE_TIMESTAMP:
                col_type = Sos.TYPE_UINT64
            if col_type == Sos.TYPE_STRUCT:
                col_type = Sos.TYPE_BYTE_ARRAY
            if col_type == typ:
                if typ in groups:
                    groups[typ].append(col)
                else:
                    groups[typ] = [ col ]
    return groups

class DebugInputer(object):
    DEF_ARRAY_LIMIT = 256
    def __init__(self, query, limit, start=0):
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
            self.dataset.append(ArrayDataByIndex(self.limit, [ col.col_name ], data))
        self.reset(start=start)

    def reset(self, start=0):
        if start:
            self.row_count = start
        else:
            self.row_count = self.start

    def input(self, row):
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

class ResultByIndexInputer(object):
    DEF_ARRAY_LIMIT = 256
    def __init__(self, query, limit, start=0):
        self.row_count = start
        self.limit = limit
        self.array_limit = self.DEF_ARRAY_LIMIT
        self.query = query
        self.dataset = DataSet()
        type_groups = group_cols_by_type(self.query.get_columns())
        for typ in type_groups:
            cols = type_groups[typ]
            typ_str = Sos.sos_type_strs[typ].lower()
            if typ >= Sos.TYPE_IS_ARRAY:
                if typ == Sos.TYPE_STRUCT:
                    typ_str = 'uint8'
                else:
                    typ_str = typ_str.replace('_array', '')
                if typ == Sos.TYPE_STRING:
                    data = np.zeros([ self.limit, len(cols) ],
                                    dtype=np.dtype('|S{0}'.format(self.DEF_ARRAY_LIMIT)))
                else:
                    data = np.zeros([ self.limit, len(cols), self.array_limit],
                                    dtype=np.dtype(typ_str))
            else:
                data = np.zeros([ self.limit, len(cols) ], dtype=np.dtype(typ_str))
            series = [ col.col_name for col in cols ]
            self.dataset.append(ArrayDataByIndex(self.limit, series, data))

    def input(self, row):
        for col in self.query.get_columns():
            col_no = self.dataset.series_idx(col.col_name)
            s = self.dataset.set_with_series_name[col.col_name]
            s_idx = self.dataset.set_col_map[col_no]
            # print("{0:12} {1:12}".format(self.row_count, s_idx), "{0:12} = {1}".format(col.col_name, col.value))
            if col.is_array or col.attr_type == Sos.TYPE_STRUCT:
                if col.attr_type != Sos.TYPE_STRING:
                    a = col.value
                    s.array[self.row_count,s_idx,:len(a)] = a
                else:
                    s.array[self.row_count,s_idx] = col.value
            elif col.attr_type != Sos.TYPE_TIMESTAMP:
                s.array[self.row_count,s_idx] = col.value
            else:
                # force timestamp to a float
                s.array[self.row_count,s_idx] = col.float_value
        self.row_count += 1
        if self.row_count == self.limit:
            return False
        return True

    def get_results(self):
        if self.row_count == 0:
            return None
        self.dataset.set_series_size(self.row_count)
        return self.dataset

class ResultByColumnInputer(object):
    def __init__(self, query, limit, start=0):
        self.row_count = start
        self.limit = limit
        self.query = query
        self.listcols = []
        self.arraycols = []
        for col in self.query.get_columns():
            if col.attr_type < Sos.TYPE_TIMESTAMP:
                self.arraycols.append(col.col_name)
            else:
                self.listcols.append(col.col_name)
        self.ndarray = np.zeros([ len(self.arraycols), self.limit ])
        self.alist = []

    def input(self, row):
        col_no = 0
        row = []
        for col in self.query.get_columns():
            if col.attr_type <= Sos.TYPE_TIMESTAMP:
                self.ndarray[ col_no ][ self.row_count ] = col.float_value
                col_no += 1
            else:
                row.append(col.value)
        self.alist.append(row)
        self.row_count += 1
        if self.row_count == self.limit:
            return False
        return True

    def get_results(self):
        if self.row_count == 0:
            return None

        result = DataSet()
        result.append(ArrayDataByColumn(self.row_count,
                                        self.arraycols, self.ndarray))
        if len(self.alist) > 0:
            result.append(ListDataByColumn(self.row_count,
                                                   self.listcols, self.alist))
        return result

class DataSource(object):

    DEF_LIMIT     = 4096
    DEF_COL_WIDTH = 16

    """Implements a generic analysis Transform data source.

    A DataSource  is a  generic interface to  a container.  An program
    does  not instantiate  the  DataSource, rather  it instantiates  a
    SosDataSource or CsvDataSource. A SosDataSource is backed by a SOS
    Container, and a CsvDataSource is backed by a text file.

    The get_results() method of a DataSource returns data as a
    DataSet. A DataSet encapsulates on or more named data series. See
    the DataSet class for more information.

    """
    def __init__(self):
        self.window = self.DEF_LIMIT
        self.col_width = self.DEF_COL_WIDTH
        self.last_result = None

    def _get_arg(self, name, args, default=None, required=True):
        if required and name not in args:
            raise ValueError("The {0} keyword argument must be specified".format(name))
        if name in args:
            return args[name]
        else:
            return default

    def _get_idx(self, attr_name):
        return self.colnames.index(attr_name)

    def set_col_width(self, col_width):
        """Set the width of columns output by the show() method"""
        self.col_width = col_width

    def get_col_width(self):
        """Get the width of columns output by the show() method"""
        return self.col_width

    def set_window(self, window):
        """Set the maximum size of a series returned in a DataSet"""
        self.window = window

    def get_window(self):
        """Return the maximum size of a series returned in a DataSet"""
        return self.window

    def config(self, **kwargs):
        """A generic interface to the sub-class's config() method"""
        raise NotImplementedError("The input_config method is not implemented")

    def select(self, attrs):
        """A generic interface to the sub-class's select() method"""
        if self.fp is None:
            raise ValueError("The data source must be configured")
        self.columns = []
        col_no = 0
        for col in attrs:
            if str == type(col):
                try:
                    c = Sos.ColSpec(col, cvt_fn=float)
                except:
                    raise ValueError("The column name '{0}' does"
                                     "not exist in {1}".format(col, self.path))
            elif Sos.ColSpec != type(col):
                raise ValueError("The attrs tuple must contain a string or a ColSpec")
            else:
                c = col
            self.columns.append(c)
            col_no += 1

    def col_by_name(self, name):
        for col in self.columns:
            if name == col.col_name:
                return col
        return None

    def get_columns(self):
        """Return the array of column definitions"""
        return self.columns

    def query(self, inputer, reset=True):
        raise ValueError("query not implemented.")

    def show(self, limit=None, file=sys.stdout, reset=True):
        """Output the data specified by the select() method

        The data is output to the sys.stdout or can be overridden with
        the 'file' keyword parameter. This is a utility function that
        makes it easy for developers to test their select() arguments
        and visually inspec the data returned.

        Keyword Parameters:

        limit -- Specifies the maximum number of rows to print. The
                 default is DataSource.DEFAULT_LIMIT.

        file  -- A Python FILE object to output data to. Default is
                 sys.stdout.

        reset -- Restart the query at 1st matching row.

        """

        if limit is None:
            limit = self.window
        last_name = None
        for col in self.get_columns():
            if last_name != col.schema_name:
                last_name = col.schema_name
                name = last_name
            else:
                name = ""
            print("{0:{width}}".format(name, width=col.width),
                  end=' ', file=file)
        print("", file=file)
        for col in self.get_columns():
            print("{0:{width}}".format(col.attr_name, width=col.width), end=' ', file=file)
        print("", file=file)
        for col in self.get_columns():
            print("{0:{width}}".format('-'.ljust(col.width, '-'), width=col.width),
                  end=' ', file=file)
        print("", file=file)

        inp = TableInputer(self, limit)
        count = self.query(inp, reset=reset)

        for col in self.get_columns():
            print("{0:{width}}".format('-'.ljust(col.width, '-'), width=col.width),
                  end=' ', file=file)
        print("\n{0} record(s)".format(count), file=file)

    def get_results(self, limit=None, wait=None, reset=True, keep=0,
                    inputer=None):

        """Return a DataSet from the DataSource

        The get_results() method returns the data identified by the
        select() method as a DataSet.

        Keyword Parameters:

        limit -- The maximum number of records to return. This limits
                 how large each series in the resulting DataSet. If
                 not specified, the limit is DataSource.window_size

        wait  -- A wait-specification that indicates how to wait for
                 results if the data available is less than
                 'limit'. See Sos.Query.query() for more information.

        reset -- Set to True to re-start the query at the beginning of
                 the matching data.

        keep  -- Return [0..keep] as the [N-keep, N] values from the
                 previous result. This is useful when the data from
                 the previous 'window' needs to be combined with the
                 the next window, for example when doing 'diff' over a
                 large series of input data, the last sample from the
                 previous window needs to be subtracted from the first
                 sample of the next window (see Transform.diff())
        """
        raise NotImplemented()

class CsvDataSource(DataSource):

    """Implements a CSV file analysis Transform data source."""
    def __init__(self):
        DataSource.__init__(self)
        self.encoding = 'utf-8'
        self.separator = ","
        self.fp = None
        self.columns = None
        self.colnames = None
        self.order = 'index'
        self.cursor = [ None ]

    def __getitem__(self, idx):
        return self.cursor[idx[1]]

    def reset(self):
        if self.fp is None:
            raise ValueError("The data source must be configured")
        self.fp.seek(0)
        line = self.fp.readline()
        if line.startswith('#'):
            line = line.strip('\n')
            cnt = line.find('#')
            line = line[cnt+1:]
            row = line.split(',')
            self.colnames = []
            idx = 0
            for col in row:
                self.colnames.append(col.strip())
                self.schema.add_attr(Csv.Attr(self.schema, col, idx, Sos.TYPE_DOUBLE))
                idx += 1
        else:
            line = line.split(self.separator)
            self.colnames = []
            for col in range(0, len(line)):
                self.colnames.append(str(col))
            self.fp.seek(0)

    def config(self, **kwargs):
        """Configure the CSV DataSource

        If the 'path' argument is specified, it is used as the
        DataSource input.  If the 'file' argument is specified, it
        refers to a Python file descriptor. The 'path' and 'file'
        arguments are mutually exclusive. If neither 'path' nor 'file'
        is specified, input is read from sys.stdin.

        Keyword Arguments:
        path      - The path to the CSV file
        file      - A Python file handle.
        schema    - The schema name for the objects (rows)
        encoding  - The text encoding of the file. The default is utf-8
        separator - The character separating columns in a CSV record,
                    the defualt is whitespace

        """
        self.path = self._get_arg('path', kwargs, required=False)
        self.file = self._get_arg('file', kwargs, required=False)
        self.encoding = self._get_arg('encoding', kwargs, required=False)
        self.separator = self._get_arg('separator', kwargs, default=',', required=False)
        self.schema_name = self._get_arg('schema', kwargs, required=True)
        self.schema = Csv.Schema(self.schema_name)

        if self.path and self.file:
            raise ValueError("The 'path' and 'file' arguments are "
                             "mutually exclusive.")

        self.fp = sys.stdin
        if self.path:
            self.fp = open(self.path, "r")
        elif self.file:
            self.fp = self.file
        self.reset()

    def show_tables(self):
        """Show all the schema available in the DataSource"""
        self.show_schemas()

    def show_schemas(self):
        """Show all the schema available in the DataSource"""
        s = self.schema
        col_len = len(s.name()) + 2

        print("{0:{width}} {1:12} {2}".format("Name", "Id", "Attr Count", width=col_len))
        print("{0:{width}} {1:12} {2:12}".format('-'.ljust(col_len, '-'), '-'.ljust(12, '-'),
                                                 '-'.ljust(12, '-'), width=col_len))
        print("{0:{width}} {1:12} {2:12}".format(s.name(), s.schema_id(), s.attr_count(),
                                                 width=col_len))

    def show_table(self, name):
        """Show a schema definition

        See show_schema().
        """
        self.show_schema(name)

    def show_schema(self, name):
        """Show the definition of a schema

        Positional Parameters:
        -- The schema name
        """
        if self.schema.name() == name:
            schema = self.schema
        else:
            schema = None
        if schema is None:
            print("The schema {0} does not exist in this DataSource.")
            return None

        print("{0:32} {1:8} {2:12} {3:8} {4}".format("Name", "Id", "Type", "Indexed", "Info"))
        print("{0:32} {1:8} {2:12} {3:8} {4}".format('-'.ljust(32, '-'),
                                                      '-'.ljust(8, '-'), '-'.ljust(12, '-'),
                                                      '-'.ljust(8, '-'), '-'.ljust(32, '-')))
        for attr in schema.attrs:
            info = None
            print("{0:32} {1:8} {2:12} {3:8} {4}".format(
                attr.name(), attr.attr_id(), attr.type_name(), str(attr.is_indexed()), info))

    def select(self, columns):
        """Specify which columns from the CSV appear in a record

        The attrs argument is an array of column-specifications.  The
        column-identifier is a string, an integer or a ColSpec()
        class.

        If it is a string, it is either the wild-card '*' or a name
        that must appear in the column header. The wild card '*' means
        all columns in the file.

        If it is an integer, it refers to the N-th column in the CSV
        file beginning with column number 0.

        If it is a ColSpec() class, please refer to the ColSpec() help
        for more information.

        Positional Parameters:
        - An array of column-specifications

        Example:

            ds.select([
                       'timestamp',
                       ColSpec('component_id', cvt_fn=int)
                       ColSpec('MemFree'),
                      ])

        """
        self.reset()
        if columns is None or columns[0] == '*':
            columns = self.colnames
        DataSource.select(self, columns)
        for c in self.columns:
            idx = self.colnames.index(c.col_name)
            c.update(self, 0, Csv.Attr(self.schema, c.col_name, idx, Sos.TYPE_DOUBLE))

    def query(self, inputer, reset=True, wait=None):
        if reset:
            self.reset()

        rec_count = 0
        while True:
            try:
                line = self.fp.next()
            except:
                return rec_count
            if line.startswith('#'):
                continue
            line = line.strip('\n')
            rec = []
            self.cursor[0] = line.split(self.separator)
            for col in self.columns:
                rec.append(col.value)
            rec_count += 1
            rc = inputer.input(rec)
            if not rc:
                break
        return rec_count

    def get_results(self, limit=None, wait=None, reset=True, keep=0,
                    inputer=None):
        if limit is None:
            limit = self.window
        if inputer is None:
            inp = Inputer.Default(self, limit, start=keep)
        else:
            inp = inputer
        if keep and self.last_result is None:
            raise ValueError("Cannot keep results from an empty previous result.")
        count = self.query(inp, reset=reset, wait=wait)
        result = inp.get_results()
        if keep:
            last_row = self.last_result.get_series_size() - keep
            for row in range(0, keep):
                for col in range(0, result.series_count):
                    result[col, row] = self.last_result[col, last_row]
                last_row += 1
        self.last_result = result
        return self.last_result

class SosDataSource(DataSource):
    """Implements a SOS DB analysis Transform data source.
    """
    def __init__(self):
        DataSource.__init__(self)
        self.cont = None
        self.schema = None
        self.query_ = None

    def reset(self):
        pass

    def get_columns(self):
        if not self.query_:
            return []
        return self.query_.get_columns()

    def config(self, **kwargs):
        """Configure the SOS data source

        Keyword Arguments:
        path      - The path to the Sos container
        cont      - A Sos.Container handle
        """
        self.path = self._get_arg('path', kwargs, required=False)
        self.cont = self._get_arg('cont', kwargs, required=False)
        if self.path == None and self.cont == None:
            raise ValueError("One of 'cont' or 'path' must be specified")
        if self.path:
            if self.cont:
                raise ValueError("The 'path' and 'cont' keywords are mutually exclusive")
            self.cont = Sos.Container(self.path, Sos.PERM_RO)

    def show_tables(self):
        """Show all the schema available in the DataSource"""
        self.show_schemas()

    def show_schemas(self):
        """Show all the schema available in the DataSource"""
        col_len = 6
        for s in self.cont.schema_iter():
            l = len(s.name())
            if l > col_len:
                col_len = l
        print("{0:{width}} {1:12} {2}".format("Name", "Id", "Attr Count", width=col_len))
        print("{0:{width}} {1:12} {2:12}".format('-'.ljust(col_len, '-'), '-'.ljust(12, '-'),
                                                 '-'.ljust(12, '-'), width=col_len))
        for s in self.cont.schema_iter():
            print("{0:{width}} {1:12} {2:12}".format(s.name(), s.schema_id(), s.attr_count(),
                                                     width=col_len))

    def show_table(self, name):
        """Show a schema definition

        See show_schema().
        """
        self.show_schema(name)

    def show_schema(self, name):
        """Show the definition of a schema

        Positional Parameters:
        -- The schema name
        """
        schema = self.cont.schema_by_name(name)
        if schema is None:
            print("The schema {0} does not exist in this DataSource.")
            return None

        print("{0:32} {1:8} {2:12} {3:8} {4}".format("Name", "Id", "Type", "Indexed", "Info"))
        print("{0:32} {1:8} {2:12} {3:8} {4}".format('-'.ljust(32, '-'),
                                                      '-'.ljust(8, '-'), '-'.ljust(12, '-'),
                                                      '-'.ljust(8, '-'), '-'.ljust(32, '-')))
        for attr in schema.attr_iter():
            info = None
            if attr.type() == Sos.TYPE_JOIN:
                join_list = attr.join_list()
                for i in join_list:
                    a = schema.attr_by_id(i)
                    if info is None:
                        info = a.name()
                    else:
                        info += ', ' + a.name()
            else:
                info = ''
            print("{0:32} {1:8} {2:12} {3:8} {4}".format(
                attr.name(), attr.attr_id(), attr.type_name(), str(attr.is_indexed()), info))

    def select(self, columns, where=None, order_by=None, from_=None, unique=False):
        """Specify which columns, order, and record selection criteria

        Positional Parameters:
        -- An array of column-specifications

        Keyword Arguments:
        from_     -- An array of schema name being queried (default is all)
        where     -- An array of query conditions
        order_by  -- The name of the attribute by which to order results
        unique    -- Return only a single result for each matching
                     the where condition

        The 'attr' keyword parameter is an array of attribute
        (i.e. column) names to include in the record.  The 'schema'
        keyword is an array of strings specifying the schema
        (i.e. tables) containing the attributes in the 'attr'
        array. The 'where' argument is an array of filter criteria
        (i.e. conditions) for the records to be returned.

        Examples:

            ds = SosDataSource()
            ds.config(path = '/DATA15/orion/ldms_data')
            ds.select([
                       'meminfo.timestamp', 'meminfo.job_id', 'meminfo.component_id',
                       'meminfo.MemFree', 'meminfo.MemAvailable',
                       'vmstat.nr_free_pages'
                      ],
                      where    = [
                                   ('job_id', Sos.COND_GT, 1),
                                   ( 'timestamp', Sos.COND_GE, ( 15451234, 0 ))
                                 ],
                      order_by = 'job_comp_time'
                     )
        """
        self.query_ = Sos.Query(self.cont)
        self.query_.select(columns, where=where, from_ = from_, order_by = order_by, unique = unique)

        col_no = 0
        self.colnames = []
        for col in self.query_.get_columns():
            self.colnames.append(col.attr_name)
            col_no += 1

    def col_by_name(self, name):
        return self.query_.col_by_name(name)

    def query(self, inputer, reset=True, wait=None):
        if self.query_:
            return self.query_.query(inputer, reset=reset, wait=wait)
        return 0

    def get_results(self, limit=None, wait=None, reset=True, keep=0,
                    inputer=None):

        """Return a DataSet from the DataSource

        The get_results() method returns the data identified by the
        select() method as a DataSet.

        Keyword Parameters:

        limit -- The maximum number of records to return. This limits
                 how large each series in the resulting DataSet. If
                 not specified, the limit is DataSource.window_size

        wait  -- A wait-specification that indicates how to wait for
                 results if the data available is less than
                 'limit'. See Sos.Query.query() for more information.

        reset -- Set to True to re-start the query at the beginning of
                 the matching data.

        keep  -- Return [0..keep] as the [N-keep, N] values from the
                 previous result. This is useful when the data from
                 the previous 'window' needs to be combined with the
                 the next window, for example when doing 'diff' over a
                 large series of input data, the last sample from the
                 previous window needs to be subtracted from the first
                 sample of the next window (see Transform.diff())
        """
        if limit is None:
            limit = self.window
        if inputer is None:
            inp = Sos.QueryInputer(self.query_, limit, start=keep)
        else:
            inp = inputer
        if keep and self.last_result is None:
            raise ValueError("Cannot keep results from an empty previous result.")
        count = self.query(inp, reset=reset, wait=wait)
        result = inp.to_dataset()
        if keep:
            last_row = self.last_result.get_series_size() - keep
            for row in range(0, keep):
                for col in range(0, result.series_count):
                    result[col, row] = self.last_result[col, last_row]
                last_row += 1
        self.last_result = result
        return self.last_result
