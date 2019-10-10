from django.db import models
import datetime as dt
import time
import os, sys, traceback, operator
from sosdb import Sos
from grafanaFormatter import DataSetFormatter
from sosdb.DataSet import DataSet
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
import numpy as np
import pandas as pd
from numsos import grafana

# Base class for grafana analysis modules
class Analysis(object):
    def __init__(self, cont, start, end, schema=None):
        self.cont = cont
        self.schema = schema
        self.start = start
        self.end = end
        self.src = SosDataSource()
        self.src.config(cont=self.cont)
        self.f = DataSetFormatter()
