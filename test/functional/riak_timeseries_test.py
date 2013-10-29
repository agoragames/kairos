'''
Functional tests for riak timeseries
'''
import time
import datetime
import os

import riak

from . import helpers
from .helpers import unittest, os

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakApiTest(helpers.ApiHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakApiTest,self).setUp()

  def test_expire(self):
    cur_time = time.time()
    self.series.insert( 'test', 1, timestamp=cur_time-600 )
    self.series.insert( 'test', 2, timestamp=cur_time-60 )
    self.series.insert( 'test', 3, timestamp=cur_time )

    kwargs = {
      'condense':True,
      'collapse':True,
      'start':cur_time-600,
      'end':cur_time
    }
    assert_equals( [1,2,3], self.series.series('test', 'minute', **kwargs).values()[0] )
    assert_equals( [1,2,3], self.series.series('test', 'hour', **kwargs).values()[0] )
    self.series.expire('test')
    assert_equals( [2,3], self.series.series('test', 'minute', **kwargs).values()[0] )
    assert_equals( [1,2,3], self.series.series('test', 'hour', **kwargs).values()[0] )

    self.series.delete('test')

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakGregorianTest(helpers.GregorianHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakGregorianTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakSeriesTest(helpers.SeriesHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakSeriesTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakHistogramTest(helpers.HistogramHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakHistogramTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakCountTest(helpers.CountHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakCountTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_RIAK','true').lower()=='true', 'skipping riak' )
class RiakGaugeTest(helpers.GaugeHelper):

  def setUp(self):
    self.client = RiakClient()
    super(RiakGaugeTest,self).setUp()
