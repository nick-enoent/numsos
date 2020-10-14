import os, sys, traceback, operator, time
import datetime as dt
from graf_analysis.grafanaAnalysis import Analysis
from numsos.DataSource import SosDataSource
from numsos.Transform import Transform
from sosdb.DataSet import DataSet
from sosdb import Sos
import time
import pandas as pd
import numpy as np
import sys
import re
import hostlist

class jobTableUser(Analysis):
	def __init__(self, cont, start, end, schema='jobid', maxDataPoints=4096):
		self.start = start
		self.end = end
		self.schema = schema
		self.cont = cont
		self.src = SosDataSource()
		self.src.config(cont=cont)
		self.maxDataPoints = maxDataPoints
		self.job_metrics = [
			'timestamp',
			'ProducerName',
			'username',
			'job_id' ]
		self.where_ = []
		if self.start > 0:
			self.where_.append(['timestamp', Sos.COND_GE, self.start])
		if self.end > 0:
			self.where_.append(['timestamp', Sos.COND_LE, self.end])

	def get_job_xfrm(self,count=4096):
		try:
			self.src.select(self.job_metrics,
				from_ = [ self.schema ],
				where = self.where_,
				order_by = 'time_job_comp'
			)	
			self.xfrm = Transform(self.src, None, limit=count)
			resp = self.xfrm.begin(count=count)
			if resp is None:
				return None
			while resp is not None:
				resp = next(self.xfrm)
				if resp is not None:
					self.xfrm.concat()
			job_times = DataSet()

			self.xfrm.min([ 'timestamp' ], group_name='job_id', \
				keep=[ 'ProducerName','username' ], xfrm_suffix='')
			job_init = self.xfrm.pop()
			job_times <<= job_init['job_id']
			job_times <<= job_init['username']
			
			a = []
			for i,c in enumerate(job_times.array('job_id')):
			     a.append(self.get_prods(job_id=int(c)))
			     stats = np.asarray(a)
			job_times.append_array(len(stats), 'num nodes', stats[:,1].astype(int))
			job_times.append_array(len(stats), 'job_start', stats[:,3].astype(int))
			job_times.append_array(len(stats), 'job_end', stats[:,4].astype(int))
			job_times.append_array(len(stats), 'job_total', stats[:,5].astype(int))
			job_times.append_array(len(stats), 'node0', stats[:,0])
			job_times.append_array(len(stats), 'nodes', stats[:,2])
			job_times.append_array(len(stats), 'Bps', stats[:,6])
			job_times.append_array(len(stats), 'File Ops', stats[:,7])
			job_times.append_array(len(stats), 'Memory', stats[:,8])
			job_times.append_array(len(stats), 'Metrics', stats[:,9])
			return job_times
		except Exception as e:
			a, b, c = sys.exc_info()
			print(str(e)+' '+str(c.tb_lineno))
			return None

	def get_prods(self, job_id=None):
		try:
			where_ = [ [ 'job_id', Sos.COND_EQ, job_id ] ]
			res = DataSet()
			src_ = SosDataSource()
			src_.config(cont=self.cont)
			src_.select(self.job_metrics,
				from_ = [ self.schema ],
				where = where_,
				order_by = 'job_time_comp'
			)
			xfrm = Transform(src_, None, limit=4096)
			resp = xfrm.begin(count=1500)
			xfrm.min([ 'job_id' ], group_name='ProducerName', xfrm_suffix='')
			res = xfrm.pop()
			num = int(len(res.array(0)))
			node0 = self.sorted_nicely(list(res.array(0)))[0]
			prods = list(res.array(0))
			#prods = [c.decode("utf-8") for c in prods]
			prodnice = hostlist.collect_hostlist(prods)
			if len(prodnice) > 25:
				prodnice = prodnice[:25] + ' ... ]'
			resp = xfrm.begin(count=4096)
			if resp is None:
				return None
			while resp is not None:
				resp = next(xfrm)
				if resp is not None:
				     xfrm.concat()
			xfrm.dup()
			xfrm.min([ 'timestamp' ], xfrm_suffix='')
			job_start = xfrm.pop()
			xfrm.max([ 'timestamp' ], xfrm_suffix='')
			job_end = xfrm.pop()
			job_total = job_end.array('timestamp') - job_start.array('timestamp')
			job_total = job_total.astype(float) / 1000000
			start_epoch = job_start.array(0)[0].astype('datetime64[ms]').astype('int')
			end_epoch = job_end.array(0)[0].astype('datetime64[ms]').astype('int')
			ret = [node0, int(num), prodnice, start_epoch, \
			     end_epoch, int(job_total[0]),"Bps", "File Ops", "Memory", "Metrics"]
			return ret
		except Exception as e: 
			a, b, c = sys.exc_info()
			print(str(e)+' '+str(c.tb_lineno))
			return None

	def sorted_nicely(self, l ): 
	    #decode = [c.decode("utf-8") for c in l]
	    """ Sort the given iterable in the way that humans expect.""" 
	    convert = lambda text: int(text) if text.isdigit() else text
	    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
	    #return sorted(decode, key = alphanum_key)
	    return sorted(l, key = alphanum_key)

	def get_data(self, metrics=None, job_id=None, user_name=None, prdcr_name=None, params=None):
		try:
			if job_id == None or job_id == 0:
				self.where_.append([ 'job_id', Sos.COND_GT, 1 ])
			else:
				self.where_.append(['job_id', Sos.COND_EQ, job_id ])
			self.user_name = user_name
			if self.user_name != None: 
				self.where_.append(['username', Sos.COND_EQ, self.user_name])
			resp = self.get_job_xfrm(count=self.maxDataPoints)
			#resp.show()
			return resp

		except Exception as e:
			a, b, c = sys.exc_info()
			print(str(e)+' '+str(c.tb_lineno))
			print(user_name)
			return None


