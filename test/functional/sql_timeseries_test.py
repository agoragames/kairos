'''
Functional tests for sql timeseries
'''
import time
import datetime
import os

from sqlalchemy import create_engine

from . import helpers
from .helpers import unittest, os
SQL_HOST = os.environ.get('SQL_HOST', 'sqlite:///:memory:')

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
