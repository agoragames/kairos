'''
Functional tests for sql timeseries
'''
import time
import datetime
import os

from sqlalchemy import create_engine

from . import helpers

SQL_HOST = os.environ.get('SQL_HOST', 'sqlite:///:memory:')
class SqlSeriesTest(helpers.SeriesTest):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlSeriesTest,self).setUp()

class SqlHistogramTest(helpers.HistogramTest):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlHistogramTest,self).setUp()

class SqlCountTest(helpers.CountTest):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlCountTest,self).setUp()

class SqlGaugeTest(helpers.GaugeTest):

  def setUp(self):
    self.client = create_engine(SQL_HOST, echo=False)
    super(SqlGaugeTest,self).setUp()
