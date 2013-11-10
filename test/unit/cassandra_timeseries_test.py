'''
Functional tests for cassandra timeseries
'''
from Queue import Queue, Empty, Full

import cql
from chai import Chai

from kairos.cassandra_backend import *

class CassandraTest(Chai):

  def setUp(self):
    super(CassandraTest,self).setUp()
    self.series = CassandraSeries.__new__(CassandraSeries, 'client')

  def test_init(self):
    client = mock()
    client.cql_major_version = 3

    attrs = ['host', 'port', 'keyspace', 'cql_version', 'compression',
      'consistency_level', 'transport', 'credentials']
    for attr in attrs:
      setattr(client, attr, attr.encode('hex'))
    
    # Trap the stuff about setting things up
    with expect( client.cursor ).returns(mock()) as c:
      expect( c.execute )
      expect( c.close )

    self.series.__init__(client)
    assert_equals( 'float', self.series._value_type )
    assert_equals( 'series', self.series._table )

    # connection pooling
    for attr in attrs:
      assert_equals( getattr(client,attr), getattr(self.series,'_'+attr) )
    assert_equals( 1, self.series._pool.qsize() )
    assert_equals( 0, self.series._pool.maxsize )
    assert_equals( client, self.series._pool.get() )

  def test_init_when_invalid_cql_version(self):
    client = mock()
    client.cql_major_version = 2
    with assert_raises( TypeError ):
      self.series.__init__(client)

  def test_init_handles_value_type(self):
    client = mock()
    client.cql_major_version = 3
    # Trap the stuff about setting things up
    with expect( client.cursor ).returns(mock()) as c:
      expect( c.execute )
      expect( c.close )
    
    self.series.__init__(client, value_type=bool)
    assert_equals( 'boolean', self.series._value_type )
    self.series.__init__(client, value_type='double')
    assert_equals( 'double', self.series._value_type )

    with assert_raises( TypeError ):
      self.series.__init__(client, value_type=object())

  def test_init_handles_table_name(self):
    client = mock()
    client.cql_major_version = 3
    # Trap the stuff about setting things up
    with expect( client.cursor ).returns(mock()) as c:
      expect( c.execute )
      expect( c.close )

    self.series.__init__(client, table_name='round')
    assert_equals( 'round', self.series._table )

  def test_init_sets_pool_size(self):
    client = mock()
    client.cql_major_version = 3
    # Trap the stuff about setting things up
    with expect( client.cursor ).returns(mock()) as c:
      expect( c.execute )
      expect( c.close )

    self.series.__init__(client, pool_size=20)
    assert_equals( 20, self.series._pool.maxsize )

  def test_connection_from_pool(self):
    self.series._pool = Queue()
    self.series._pool.put('conn')
    assert_equals( 'conn', self.series._connection() )
    assert_equals( 0, self.series._pool.qsize() )

  def test_connection_when_pool_is_empty(self):
    self.series._pool = Queue(2)
    self.series._host = 'dotcom'
    self.series._port = 'call'
    self.series._keyspace = 'gatekeeper'
    self.series._cql_version = '3.2.1'
    self.series._compression = 'smaller'
    self.series._consistency_level = 'most'
    self.series._transport = 'thrifty'
    self.series._credentials = {'user':'you', 'password':'knockknock'}

    expect( cql, 'connect' ).args( 'dotcom', 'call', 'gatekeeper',
      user='you', password='knockknock', cql_version='3.2.1',
      compression='smaller', consistency_level='most', 
      transport='thrifty' ).returns( 'conn' )

    assert_equals( 'conn', self.series._connection() )

  def test_return_when_pool_not_full(self):
    self.series._pool = Queue(1)
    assert_equals( 0, self.series._pool.qsize() )
    self.series._return( 'conn' )
    assert_equals( 1, self.series._pool.qsize() )

  def test_return_when_pool_full(self):
    self.series._pool = Queue(2)
    self.series._return( 'a' )
    self.series._return( 'b' )
    self.series._return( 'c' )
    assert_equals( 2, self.series._pool.qsize() )
    assert_equals( 'a', self.series._pool.get() )
    assert_equals( 'b', self.series._pool.get() )
