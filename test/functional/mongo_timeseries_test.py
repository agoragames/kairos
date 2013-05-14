'''
Functional tests for mongo timeseries
'''
import time
import datetime

from pymongo import *
from chai import Chai

import helpers

class MongoSeriesTest(helpers.SeriesTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoSeriesTest,self).setUp()

class MongoHistogramTest(helpers.HistogramTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoHistogramTest,self).setUp()

class MongoCountTest(helpers.CountTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoCountTest,self).setUp()

class MongoGaugeTest(helpers.GaugeTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoGaugeTest,self).setUp()

class MongoGregorianTest(helpers.GregorianTest):

  def setUp(self):
    self.client = MongoClient('localhost')
    super(MongoGregorianTest,self).setUp()
