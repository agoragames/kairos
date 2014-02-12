'''
Functional tests for redis timeseries
'''
import time
import datetime

import redis
from chai import Chai

from . import helpers
from .helpers import unittest, os, Timeseries

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisApiTest(helpers.ApiHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisApiTest,self).setUp()

  def test_url_parse(self):
    assert_equals( 'RedisSeries', 
      Timeseries('redis://', type='series').__class__.__name__ )

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisGregorianTest(helpers.GregorianHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGregorianTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisSeriesTest(helpers.SeriesHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSeriesTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisHistogramTest(helpers.HistogramHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisHistogramTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisCountTest(helpers.CountHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisCountTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisGaugeTest(helpers.GaugeHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGaugeTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_REDIS','true').lower()=='true', 'skipping redis' )
class RedisSetTest(helpers.SetHelper):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSetTest,self).setUp()
