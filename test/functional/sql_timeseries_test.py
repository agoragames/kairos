'''
Functional tests for sql timeseries
'''
import time
import datetime
import os

from sqlalchemy import create_engine

from . import helpers
from .helpers import unittest, os, Timeseries

SQL_HOST = os.environ.get('SQL_HOST', 'sqlite:///:memory:')

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlApiTest(helpers.ApiHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlApiTest,self).setUp()

  def test_url_parse(self):
    assert_equals( 'SqlSeries', 
      Timeseries('sqlite:///:memory:', type='series').__class__.__name__ )

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

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlGregorianTest(helpers.GregorianHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlGregorianTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlSeriesTest(helpers.SeriesHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlSeriesTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlHistogramTest(helpers.HistogramHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlHistogramTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlCountTest(helpers.CountHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlCountTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_SQL','true').lower()=='true', 'skipping sql' )
class SqlGaugeTest(helpers.GaugeHelper):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlGaugeTest,self).setUp()
