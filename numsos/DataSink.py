from __future__ import print_function
import numpy as np
from sosdb import Sos
from numsos import DataSet, Csv
import datetime as dt
import copy
import time
import os
import sys

class DataSink(object):
    """Implements a generic analysis Transform data sink

    A Transform data sink writes a DataSet data to a storage
    backend.
    """
    def __init__(self):
        self.col_width = 16
        self.initialize = True
        self.row_no = 0
        self.results = None
        self.columns = []

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

    def config(self, **kwargs):
        raise NotImplementedError("The input_config method is not implemented")

    def insert(self, columns):
        """Insert the columns from the DataSet that will be saved.

        Specify which columns from the result are stored. The
        column-specification array provides the order and series
        name of each record from the result that is stored.

        If this function is not called, all series in the
        result record will be stored in the order present in the
        result.
        """
        self.columns = []
        for col in columns:
            if str == type(col):
                try:
                    c = Sos.ColSpec(col, cvt_fn=None)
                except:
                    raise ValueError("The column name '{0}' does"
                                     "not exist in {1}".format(col, self.path))
            elif Sos.ColSpec != type(col):
                raise ValueError("The columns[] must contain a string or a ColSpec")
            else:
                c = copy.copy(col)
            self.columns.append(c)

    def __getitem__(self, idx):
        return self.results[idx[0], self.row_no]

    def put_results(self, results):
        """Saves DataSet results to a DataSink container. """
        raise ValueError("Not implemented!")

class CsvDataSink(DataSink):
    """Implements a CSV file analysis Transform data source.
    """
    def __init__(self):
        DataSink.__init__(self)
        self.encoding = 'utf-8'
        self.separator = ","
        self.fp = None
        self.colnames = None

    def config(self, **kwargs):
        """Configure the Transform input data source

        If the 'path' argument is specified, it is used as the
        DataSink output file. The file is created if it does not
        already exist.  If the 'file' argument is specified, it refers
        to a Python file descriptor. The 'path' and 'file' arguments
        are mutually exclusive. If neither 'path' nor 'file' is
        specified, output is written to sys.stdout.

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
        self.encoding = self._get_arg('encoding', kwargs, default='utf-8', required=False)
        self.separator = self._get_arg('separator', kwargs, default=',', required=False)
        self.header = self._get_arg('header', kwargs, default=True, required=False)

        if self.path and self.file:
            raise ValueError("The 'path' and 'file' arguments are "
                             "mutually exclusive.")

        self.fp = sys.stdout
        if self.path:
            self.fp = open(self.path, "r+")
        elif self.file:
            self.fp = self.file

    def insert(self, columns, into=None):
        if into is None:
            raise ValueError("The 'into' keyword parameter must be specified.")
        self.schema_name = into
        self.schema = Csv.Schema(self.schema_name)

        DataSink.insert(self, columns)
        if self.header:
            self.fp.write("# " + self.columns[0].col_name.replace("#", "@"))
            for c in self.columns[1:]:
                self.fp.write(", " + c.col_name.replace("#", "@"))
            self.fp.write("\n")

    def put_results(self, results):
        """Save DataSet results."""
        if self.initialize:
            for c in self.columns:
                # Update the ColSpec with the "attr_id" which in this
                # case is the result series index
                idx = results.series.index(c.col_name)
                c.update(self, idx, Csv.Attr(self.schema, c.col_name, idx, Sos.TYPE_STRING))
            self.initialize = False

        self.results = results
        for self.row_no in range(0, results.get_series_size()):
            self.fp.write(str(self.columns[0].value))
            for col in self.columns[1:]:
                self.fp.writelines( [ ",", str(col.value) ] )
            self.fp.write("\n")

class SosDataSink(DataSink):
    Metric_Columns = [
            Sos.ColSpec("timestamp"),
            Sos.ColSpec("component_id", cvt_fn=int),
            Sos.ColSpec("job_id", cvt_fn=int),
    ]
    Metric_Attrs = [
        { "name" : "timestamp",    "type" : "timestamp", "index" : {} },
        { "name" : "component_id", "type" : "uint64",    "index" : {} },
        { "name" : "job_id",       "type" : "uint64",    "index" : {} }
    ]

    Metric_Joins = [
        { "name" : "comp_time", "type" : "join",
          "join_attrs" : [ "component_id", "timestamp" ],
          "index" : {} },
        { "name" : "job_comp_time", "type" : "join",
          "join_attrs" : [ "job_id", "component_id", "timestamp" ],
          "index" : {} },
        { "name" : "job_time_comp", "type" : "join",
          "join_attrs" : [ "job_id", "timestamp", "component_id" ],
          "index" : {} }
    ]

    """Implements a SOS database analysis Transform data sink."""
    def __init__(self):
        DataSink.__init__(self)
        self.cont = None
        self.schema = None

    def get_columns(self):
        if not self.query_:
            return []
        return self.query_.get_columns()

    def config(self, **kwargs):
        """Configure the SOS data sink

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

            try:
                self.cont = Sos.Container(path=self.path, o_perm=Sos.PERM_RW)
            except:
                self.cont = Sos.Container()
                create = self._get_arg('create', kwargs, required=False, default=False)
                mode = self._get_arg('mode', kwargs, required=False, default=0664)
                if create:
                    # Create the database
                    self.cont.create(path=self.path, o_mode=mode)
                    self.cont.open(self.path, o_perm=Sos.PERM_RW)
                    self.cont.part_create("ROOT")
                    part = self.cont.part_by_name("ROOT")
                    part.state_set("primary")
                else:
                    raise ValueError("The container {0} does not exist.".format(self.path))

    def show_tables(self):
        """Show all the schema available in the DataSink"""
        self.show_schemas()

    def show_schemas(self):
        """Show all the schema available in the DataSink"""
        print("{0:32} {1:12} {2}".format("Name", "Id", "Attr Count"))
        print("{0:32} {1:12} {2:12}".format('-'.ljust(32, '-'),
                                            '-'.ljust(12, '-'),
                                            '-'.ljust(12, '-')))
        for s in self.cont.schema_iter():
            print("{0:32} {1:12} {2:12}".format(s.name(),
                                                s.schema_id(),
                                                s.attr_count()))

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
            print("The schema {0} does not exist in this DataSink.")
            return None

        print("{0:32} {1:8} {2:12} {3:8} {4}".format("Name", "Id", "Type", "Indexed",
                                                     "Info"))
        print("{0:32} {1:8} {2:12} {3:8} {4}".format('-'.ljust(32, '-'),
                                                     '-'.ljust(8, '-'),
                                                     '-'.ljust(12, '-'),
                                                     '-'.ljust(8, '-'),
                                                     '-'.ljust(32, '-')))
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
                attr.name(), attr.attr_id(), attr.type_name(),
                str(attr.is_indexed()), info))

    def add_schema(self, name, template):
        sch = Sos.Schema()
        sch.from_template(name, template)
        sch.add(self.cont)

    def get_schema(self, name):
        """Return Schema object if present in DataSink"""
        return self.cont.schema_by_name(name)

    def insert(self, columns, into=None):
        """Specifies the result data to be output

        The 'into' keyword parameter is either a string specifying an
        existing schema name or a dictionary. If it is a string, the
        schema must alrady exist in the container, and the schema
        definition must be suitable to contain the output data
        specified in the column-specification list. If it is a
        dictionary, it must contain a 'schema' entry specifying the
        schema name, and an 'attrs' entry providing the
        attribute-specifications. In this case, the schema will be
        created if it does not already exist.

        The schema definitions in this DataSink are normalized to
        contain three indexed data attributes "timestamp", "component_id",
        and "job_id", and three indexed join attributes "comp_time",
        "job_comp_time", and "job_time_comp". These attributes will be
        added automatically if they are not provided in the schema
        definition.

        Positional Parameters:
        -- A list of ColSpec specifying the names of the series in the
           result to be saved.

        Keyword Parameters:
        into -- A string specifying an existing schema name or
                a schema definition dictionary

        """
        DataSink.insert(self, columns)
        if into is None:
            raise ValueError("The 'into' keyword parameter is required.")

        if type(into) == str:
            self.schema = self.cont.schema_by_name(into)
            if self.schema is None:
                raise ValueError("The schema {0} does not exist in the DataSink.".format(into))
        else:
            self.schema = self.cont.schema_by_name(into["schema"])

        if self.schema is None:
            schema = Sos.Schema()
            schema.from_template(into['schema'], into['attrs'])
            schema.add(self.cont)
            self.schema = schema

    def put_results(self, results):
        """Save DataSet results."""
        if self.initialize:
            for c in self.columns:
                idx = results.series.index(c.col_name)
                c.update(self, idx, self.schema.attr_by_name(c.col_name))
            self.initialize = False
        self.results = results
        for self.row_no in range(0, results.get_series_size()):
            # If the object exists, use it, if not, allocate a new object
            obj = self.get_object(results
            obj = self.schema.alloc()
            for col in self.columns:
                obj[col.attr_id] = col.value
            obj.index_add()

