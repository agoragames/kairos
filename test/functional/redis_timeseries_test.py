'''
Functional tests for redis timeseries
'''
import time
import datetime

import redis
from chai import Chai

from helpers import *

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisSeriesTest(SeriesTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSeriesTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisHistogramTest(HistogramTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisHistogramTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisCountTest(CountTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisCountTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisGaugeTest(GaugeTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGaugeTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisSetTest(SetTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisSetTest,self).setUp()

@unittest.skipIf( os.environ.get('SKIP_REDIS','').lower()=='true', 'skipping redis' )
class RedisGregorianTest(GregorianTest):

  def setUp(self):
    self.client = redis.Redis('localhost')
    super(RedisGregorianTest,self).setUp()
