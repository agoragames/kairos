'''
Functional tests for sql timeseries
'''
import time
import datetime

from sqlalchemy import create_engine

from . import helpers

class SqlSeriesTest(helpers.SeriesTest):

  def setUp(self):
    self.client = create_engine('sqlite:///:memory:', echo=False)
    super(SqlSeriesTest,self).setUp()

class SqlHistogramTest(helpers.HistogramTest):

  def setUp(self):
    self.client = create_engine('sqlite:///:memory:', echo=False)
    super(SqlHistogramTest,self).setUp()

class SqlCountTest(helpers.CountTest):

  def setUp(self):
    self.client = create_engine('sqlite:///:memory:', echo=False)
    super(SqlCountTest,self).setUp()

class SqlGaugeTest(helpers.GaugeTest):

  def setUp(self):
    self.client = create_engine('sqlite:///:memory:', echo=False)
    super(SqlGaugeTest,self).setUp()
