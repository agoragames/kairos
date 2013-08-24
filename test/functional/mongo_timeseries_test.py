'''
Functional tests for mongo timeseries
'''
import time
import datetime

from pymongo import *
from chai import Chai

from helpers import *

@unittest.skipIf( os.environ.get('SKIP_MONGO','').lower()=='true', 'skipping mongo' )
class MongoSeriesTest(helpers.SeriesTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoSeriesTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_MONGO','').lower()=='true', 'skipping mongo' )
class MongoHistogramTest(helpers.HistogramTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoHistogramTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_MONGO','').lower()=='true', 'skipping mongo' )
class MongoCountTest(helpers.CountTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoCountTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_MONGO','').lower()=='true', 'skipping mongo' )
class MongoGaugeTest(helpers.GaugeTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoGaugeTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_MONGO','').lower()=='true', 'skipping mongo' )
class MongoGregorianTest(helpers.GregorianTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoGregorianTest,self).setUp()
