'''
Functional tests for cassandra timeseries
'''
import time
import datetime
import os

import cql

from . import helpers
from .helpers import unittest, os, Timeseries

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraApiTest(helpers.ApiHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraApiTest,self).setUp()

  def test_url_parse(self):
    assert_equals( 'CassandraSeries', 
      Timeseries( 'cql://localhost', type='series' ).__class__.__name__ )

# Not running gregorian tests because they run in the "far future" where long
# TTLs are not supported.
#@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
#class CassandraGregorianTest(helpers.GregorianHelper):

  #def setUp(self):
    #self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    #super(CassandraGregorianTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraSeriesTest(helpers.SeriesHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraSeriesTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraHistogramTest(helpers.HistogramHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraHistogramTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraCountTest(helpers.CountHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraCountTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraGaugeTest(helpers.GaugeHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraGaugeTest,self).setUp()

@unittest.skipUnless( os.environ.get('TEST_CASSANDRA','true').lower()=='true', 'skipping cassandra' )
class CassandraSetTest(helpers.SetHelper):

  def setUp(self):
    self.client = cql.connect('localhost', 9160, os.environ.get('CASSANDRA_KEYSPACE','kairos'), cql_version='3.0.0')
    super(CassandraSetTest,self).setUp()
