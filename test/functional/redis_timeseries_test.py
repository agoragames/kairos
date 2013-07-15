'''
Functional tests for redis timeseries
'''
import time
import datetime

import redis
from chai import Chai

import helpers

class RedisSeriesTest(helpers.SeriesTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSeriesTest,self).setUp()

class RedisHistogramTest(helpers.HistogramTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisHistogramTest,self).setUp()

class RedisCountTest(helpers.CountTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisCountTest,self).setUp()

class RedisGaugeTest(helpers.GaugeTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGaugeTest,self).setUp()

class RedisSetTest(helpers.SetTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSetTest,self).setUp()

class RedisGregorianTest(helpers.GregorianTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGregorianTest,self).setUp()
