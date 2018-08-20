from __future__ import print_function
import numpy as np
from sosdb import Sos
from numsos import DataSet, Csv
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

class ResultByIndexInputer(object):
    def __init__(self, query, limit, start=0):
        self.row_count = start
        self.limit = limit
        self.query = query
        self.listcols = []
        self.arraycols = []
        for col in self.query.get_columns():
            if col.attr_type <= Sos.TYPE_TIMESTAMP:
                self.arraycols.append(col.col_name)
            else:
                self.listcols.append(col.col_name)
        self.ndarray = np.zeros([ self.limit, len(self.arraycols) ])
        self.alist = []

    def input(self, row):
        col_no = 0
        row = []
        for col in self.query.get_columns():
            if col.attr_type <= Sos.TYPE_TIMESTAMP:
                self.ndarray[ self.row_count ][ col_no ] = col.float_value
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
        result = DataSet.DataSet()
        result.append(DataSet.ArrayDataByIndex(self.row_count,
                                        self.arraycols, self.ndarray))
        if len(self.alist) > 0:
            result.append(DataSet.ListDataByIndex(self.row_count,
                                                  self.listcols, self.alist))
        return result

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

        result = DataSet.DataSet()
        result.append(DataSet.ArrayDataByColumn(self.row_count,
                                        self.arraycols, self.ndarray))
        if len(self.alist) > 0:
            result.append(DataSet.ListDataByColumn(self.row_count,
                                                   self.listcols, self.alist))
        return result

class DataSource(object):
    """Implements a generic analysis Transform data source.

    A Transform data source returns one or more records to the caller
    of the input function. A record is a logical datum from the data
    store and is defined by the select() method. Each record consists
    of one or more attributes that are contained in the data store. An
    attribute is logically a column in an SQL table or an object
    attribute in an object store. See the select() method for more
    information.

    A DataSource object is configured with a window-size that
    specifies the maximum number of records to be returned in each
    call to input(). See the input() documentation for more
    information.

    The filter() method is used to define a set of conditions that
    each record returned by input() must meet. These conditions are
    specified as a set of logical expressions. See the filter() method
    for more information.
    """
    def __init__(self):
        self.window = 1024
        self.col_width = 16
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
        self.col_width = col_width

    def get_col_width(self):
        return self.col_width

    def set_window(self, window):
        """Set the maximum number of records to return by each
        call to input()."""
        self.window = window

    def get_window(self):
        """Return the maximum number of records returned by each call
        to input()"""
        return self.window

    def config(self, **kwargs):
        raise NotImplementedError("The input_config method is not implemented")

    def select(self, attrs):
        """Specify column specifications

        Logically each element from the data source is a record
        containing one or more named attributes. The
        column-specification array specifies the order and attribute
        name of each record to be returned.

        If this function is not called, all columns/attributes in the
        data source will be returned by input()
        """
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

    def get_columns(self):
        return self.columns

    def query(self, inputer, reset=True):
        raise ValueError("query not implemented.")

    def show(self, limit=None, file=sys.stdout, reset=True):
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

    def get_results(self, limit=None, wait=None, reset=True, order='index', keep=0):
        """Return a DataSet from the DataSource

        Keyword Parameters:

        limit  -- The maximum number of records to return
        wait   -- A wait-specification that indicates how to wait for
                  results if the data available is less than 'limit'
        result -- Set to True to start at the beginning of the matching data
        keep   -- Return [0..keep] as the [N-keep, N] values from the previous result.
        """
        if limit is None:
            limit = self.window
        if order == 'index':
            inp = ResultByIndexInputer(self, limit, start=keep)
        else:
            inp = ResultByColumnInputer(self, limit, start=keep)
        if keep and self.last_result is None:
            raise ValueError("Cannot keep results from an empty previous result.")
        count = self.query(inp, reset=reset, wait=wait)
        result = inp.get_results()
        if keep:
            last_row = len(self.last_result) - keep
            for row in range(0, keep):
                for col in range(0, len(result.series)):
                    result[col, row] = self.last_result[col, last_row]
                last_row += 1
        self.last_result = result
        return self.last_result

class CsvDataSource(DataSource):

        """Implements a CSV file analysis Transform data source.
    """
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
            for col in row:
                self.colnames.append(col.strip())
        else:
            line = line.split(self.separator)
            self.colnames = []
            for col in range(0, len(line)):
                self.colnames.append(str(col))
            self.fp.seek(0)

    def config(self, **kwargs):
        """Configure the Transform input data source

        Keyword Arguments:
        path      - The path to the CSV file
        schema    - The schema name for the objects (rows)
        encoding  - The text encoding of the file. The default is utf-8
        separator - The character separating columns in a CSV record,
                    the defualt is whitespace
        """
        self.path = self._get_arg('path', kwargs)
        self.encoding = self._get_arg('encoding', kwargs, required=False)
        self.separator = self._get_arg('separator', kwargs, default=',', required=False)
        self.schema_name = self._get_arg('schema', kwargs, required=True)
        self.schema = Csv.Schema(self.schema_name)

        self.fp = open(self.path, "r")
        self.reset()

    def select(self, columns):
        """Specify which columns from the CSV appear in a record

        The attrs argument is an array of column-specifications. A
        column-specification is either a string or a list. If
        column-specification is a list, is consists of a
        column-identifer, and a column-converter. If it is not a list,
        it is a column-identifier.

        The column-identifier is a string or an integer. If it is a
        string, it refers to column number X in a CSV line where
        column numbers begin at 0. If it is a string, it is the name
        of a column and the CSV file MUST contain a header comment
        beginning with the character '#' that enumerates the names of
        each column in the file separated by commas.

        The column-converter is a type conversion function that will
        be used to convert the CSV column text to a value in the
        record. If the column-converter is not specified, the builtin
        Python function float() will be used.

        Positional Parameters:
        - An array of column-specifications

        Example:

        ds.select([
                       'timestamp',
                       ColSpec('component_id', cvt_fn=int)
                       ColSpec('MemFree'),
                  ]
                 )

        """
        self.reset()
        if columns is None:
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
        print("{0:32} {1:12} {2}".format("Name", "Id", "Attr Count"))
        print("{0:32} {1:12} {2:12}".format('-'.ljust(32, '-'), '-'.ljust(12, '-'), '-'.ljust(12, '-')))
        for s in self.cont.schema_iter():
            print("{0:32} {1:12} {2:12}".format(s.name(), s.schema_id(), s.attr_count()))

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

    def select(self, columns, where=None, order_by=None, from_=None):
        """Specify which columns, order, and record selection criteria

        Positional Parameters:
        -- An array of column-specifications

        Keyword Arguments:
        from_     -- An array of schema name being queried (default is all)
        where     -- An array of query conditions
        order_by  -- The name of the attribute by which to order results

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
        self.query_.select(columns, where=where, from_ = from_, order_by = order_by)

        col_no = 0
        self.colnames = []
        for col in self.query_.get_columns():
            self.colnames.append(col.attr_name)
            col_no += 1

    def query(self, inputer, reset=True, wait=None):
        if self.query_:
            return self.query_.query(inputer, reset=reset, wait=wait)
        return 0


