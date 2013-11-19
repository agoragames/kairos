'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from .exceptions import *
from .timeseries import *

import cql

import time
from datetime import date, datetime
from datetime import time as time_type
from decimal import Decimal
from Queue import Queue, Empty, Full
import re

# Test python3 compatibility
try:
  x = long(1)
except NameError:
  long = int
try:
  x = unicode('foo')
except NameError:
  unicode = str

TYPE_MAP = {
  str         : 'ascii',
  'str'       : 'ascii',
  'string'    : 'ascii',

  unicode     : 'text',  # works for py3 too
  'unicode'   : 'text',

  float       : 'float',
  'float'     : 'float',

  'double'    : 'double',

  int         : 'int',
  'int'       : 'int',
  'integer'   : 'int',

  long        : 'varint', # works for py3 too
  'long'      : 'varint',
  'int64'     : 'bigint',
  
  'decimal'   : 'decimal',

  bool        : 'boolean',
  'bool'      : 'boolean',
  'boolean'   : 'boolean',

  'text'      : 'text',
  'clob'      : 'blob',
  'blob'      : 'blob',

  'inet'      : 'inet',
}

QUOTE_TYPES = set(['ascii','text','blob'])
QUOTE_MATCH = re.compile("^'.*'$")

def scoped_connection(func):
  '''
  Decorator that gives out connections.
  '''
  def _with(series, *args, **kwargs):
    connection = None
    try:
      connection = series._connection()
      return func(series, connection, *args, **kwargs)
    finally:
      series._return( connection )
  return _with

class CassandraBackend(Timeseries):
  
  def __new__(cls, *args, **kwargs):
    if cls==CassandraBackend:
      ttype = kwargs.pop('type', None)
      if ttype=='series':
        return CassandraSeries.__new__(CassandraSeries, *args, **kwargs)
      elif ttype=='histogram':
        return CassandraHistogram.__new__(CassandraHistogram, *args, **kwargs)
      elif ttype=='count':
        return CassandraCount.__new__(CassandraCount, *args, **kwargs)
      elif ttype=='gauge':
        return CassandraGauge.__new__(CassandraGauge, *args, **kwargs)
      elif ttype=='set':
        return CassandraSet.__new__(CassandraSet, *args, **kwargs)
    return Timeseries.__new__(cls, *args, **kwargs)
  
  def __init__(self, client, **kwargs):
    '''
    Initialize the sql backend after timeseries has processed the configuration.
    '''
    # Only CQL3 is supported
    if client.cql_major_version != 3:
      raise TypeError("Only CQL3 is supported")
    
    vtype = kwargs.get('value_type', float)
    if vtype in TYPE_MAP:
      self._value_type = TYPE_MAP[vtype]
    else:
      raise TypeError("Unsupported type '%s'"%(vtype))

    self._table = kwargs.get('table_name', self._table)

    # copy internal variables of the connection for poor-mans pooling
    self._host = client.host
    self._port = client.port
    self._keyspace = client.keyspace
    self._cql_version = client.cql_version
    self._compression = client.compression
    self._consistency_level = client.consistency_level
    self._transport = client.transport
    self._credentials = client.credentials
    self._pool = Queue(kwargs.get('pool_size',0))
    self._pool.put( client )
    
    super(CassandraBackend,self).__init__(client, **kwargs)

  def _connection(self):
    '''
    Return a connection from the pool
    '''
    try:
      return self._pool.get(False)
    except Empty:
      args = [
        self._host, self._port, self._keyspace
      ]
      kwargs = {
        'user'              : None,
        'password'          : None,
        'cql_version'       : self._cql_version,
        'compression'       : self._compression,
        'consistency_level' : self._consistency_level,
        'transport'         : self._transport,
      }
      if self._credentials:
        kwargs['user'] = self._credentials['user']
        kwargs['password'] = self._credentials['password']
      return cql.connect(*args, **kwargs)

  def _return(self, connection):
    try:
      self._pool.put(connection, False)
    except Full:
      # do not return connection to the pool. 
      pass
  
  def _insert(self, name, value, timestamp, intervals):
    '''
    Insert the new value.
    '''
    if self._value_type in QUOTE_TYPES and not QUOTE_MATCH.match(value):
      value = "'%s'"%(value)
      
    for interval,config in self._intervals.items():
      self._insert_data(name, value, timestamp, interval, config)
      steps = intervals
      if steps<0:
        while steps<0:
          i_timestamp = config['i_calc'].normalize(timestamp, steps)
          self._insert_data(name, value, i_timestamp, interval, config)
          steps += 1
      elif steps>0:
        while steps>0:
          i_timestamp = config['i_calc'].normalize(timestamp, steps)
          self._insert_data(name, value, i_timestamp, interval, config)
          steps -= 1
  
  @scoped_connection
  def _get(self, connection, name, interval, config, timestamp, **kws):
    '''
    Get the interval.
    '''
    i_bucket = config['i_calc'].to_bucket(timestamp)
    fetch = kws.get('fetch')
    process_row = kws.get('process_row') or self._process_row

    rval = OrderedDict()
    if fetch:
      data = fetch( connection, self._table, name, interval, [i_bucket] )
    else:
      data = self._type_get(name, interval, i_bucket)

    if config['coarse']:
      if data:
        rval[ config['i_calc'].from_bucket(i_bucket) ] = process_row(data.values()[0][None])
      else:
        rval[ config['i_calc'].from_bucket(i_bucket) ] = self._type_no_value()
    else:
      for r_bucket,row_data in data.values()[0].items():
        rval[ config['r_calc'].from_bucket(r_bucket) ] = process_row(row_data)
    
    return rval
  
  @scoped_connection
  def _series(self, connection, name, interval, config, buckets, **kws):
    '''
    Fetch a series of buckets.
    '''
    fetch = kws.get('fetch')
    process_row = kws.get('process_row') or self._process_row

    rval = OrderedDict()

    if fetch:
      data = fetch( connection, self._table, name, interval, buckets )
    else:
      data = self._type_get(name, interval, buckets[0], buckets[-1])

    if config['coarse']:
      for i_bucket in buckets:
        i_key = config['i_calc'].from_bucket(i_bucket)
        i_data = data.get( i_bucket )
        if i_data:
          rval[ i_key ] = process_row( i_data[None] )
        else:
          rval[ i_key ] = self._type_no_value()
    else:
      if data:
        for i_bucket, i_data in data.items():
          i_key = config['i_calc'].from_bucket(i_bucket)
          rval[i_key] = OrderedDict()
          for r_bucket, r_data in i_data.items():
            r_key = config['r_calc'].from_bucket(r_bucket)
            if r_data:
              rval[i_key][r_key] = process_row(r_data)
            else:
              rval[i_key][r_key] = self._type_no_value()
    
    return rval
  
  @scoped_connection
  def delete(self, connection, name):
    cursor = connection.cursor()
    try:
      cursor.execute("DELETE FROM %s WHERE name='%s';"%(self._table,name))
    finally:
      cursor.close()

  @scoped_connection
  def delete_all(self, connection):
    cursor = connection.cursor()
    try:
      cursor.execute("TRUNCATE %s"%(self._table))
    finally:
      cursor.close()
  
  @scoped_connection
  def list(self, connection):
    cursor = connection.cursor()
    rval = set()

    try:
      cursor.execute('SELECT name FROM %s'%(self._table))
      for row in cursor:
        rval.add(row[0])
    finally:
      cursor.close()
    return list(rval)

  @scoped_connection
  def properties(self, connection, name):
    cursor = connection.cursor()
    rval = {}

    try:
      for interval,config in self._intervals.items():
        rval.setdefault(interval, {})

        cursor.execute('''SELECT i_time 
          FROM %s
          WHERE name = '%s' AND interval = '%s'
          ORDER BY interval ASC, i_time ASC
          LIMIT 1'''%(self._table, name, interval))
          
        rval[interval]['first'] = config['i_calc'].from_bucket(
          cursor.fetchone()[0] )
        
        cursor.execute('''SELECT i_time 
          FROM %s
          WHERE name = '%s' AND interval = '%s'
          ORDER BY interval DESC, i_time DESC
          LIMIT 1'''%(self._table, name, interval))
        rval[interval]['last'] = config['i_calc'].from_bucket(
          cursor.fetchone()[0] )
    finally:
      cursor.close()

    return rval

class CassandraSeries(CassandraBackend, Series):
  
  def __init__(self, *a, **kwargs):
    self._table = 'series'
    super(CassandraSeries,self).__init__(*a, **kwargs)

    cursor = self._client.cursor()
    # TODO: support other value types
    # TODO: use varint for [ir]_time?
    try:
      res = cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        name text,
        interval text,
        i_time bigint,
        r_time bigint,
        value list<%s>,
        PRIMARY KEY(name, interval, i_time, r_time)
      )'''%(self._table, self._value_type))
    except cql.ProgrammingError as pe:
      if 'existing' not in str(pe):
        raise
    finally:
      cursor.close()
  
  @scoped_connection
  def _insert_data(self, connection, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = -1
   
    # TODO: figure out escaping rules of CQL
    cursor = connection.cursor()
    try:
      table_spec = self._table
      expire = config['expire']
      if expire:
        table_spec += " USING TTL %s "%(expire)
      stmt = '''UPDATE %s SET value = value + [%s]
        WHERE name = '%s'
        AND interval = '%s'
        AND i_time = %s
        AND r_time = %s'''%(table_spec, value, name, interval, i_time, r_time)
      cursor.execute(stmt)
    finally:
      cursor.close()

  @scoped_connection
  def _type_get(self, connection, name, interval, i_bucket, i_end=None):
    rval = OrderedDict()
    
    # TODO: more efficient creation of query string
    stmt = '''SELECT i_time, r_time, value 
      FROM %s
      WHERE name = '%s' AND interval = '%s'
    '''%(self._table, name, interval)
    if i_end :
      stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
    else:
      stmt += ' AND i_time = %s'%(i_bucket)
    stmt += ' ORDER BY interval, i_time, r_time'

    cursor = connection.cursor()
    try:
      cursor.execute(stmt)
      for row in cursor:
        i_time, r_time, value = row
        if r_time==-1:
          r_time = None
        rval.setdefault(i_time,OrderedDict())[r_time] = value
    finally:
      cursor.close()
    return rval

class CassandraHistogram(CassandraBackend, Histogram):
  
  def __init__(self, *a, **kwargs):
    self._table = 'histogram'
    super(CassandraHistogram,self).__init__(*a, **kwargs)

    # TODO: use varint for [ir]_time?
    # TODO: support other value types
    cursor = self._client.cursor()
    try:
      res = cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        name text,
        interval text,
        i_time bigint,
        r_time bigint,
        value %s,
        count counter,
        PRIMARY KEY(name, interval, i_time, r_time, value)
      )'''%(self._table, self._value_type))
    except cql.ProgrammingError as pe:
      if 'existing' not in str(pe):
        raise
    finally:
      cursor.close()
  
  @scoped_connection
  def _insert_data(self, connection, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = -1
   
    # TODO: figure out escaping rules of CQL
    cursor = connection.cursor()
    try:
      table_spec = self._table
      expire = config['expire']
      if expire:
        table_spec += " USING TTL %s "%(expire)
      stmt = '''UPDATE %s SET count = count + 1
        WHERE name = '%s'
        AND interval = '%s'
        AND i_time = %s
        AND r_time = %s
        AND value = %s'''%(table_spec, name, interval, i_time, r_time, value)
      cursor.execute(stmt)
    finally:
      cursor.close()

  @scoped_connection
  def _type_get(self, connection, name, interval, i_bucket, i_end=None):
    rval = OrderedDict()
    
    # TODO: more efficient creation of query string
    stmt = '''SELECT i_time, r_time, value, count
      FROM %s
      WHERE name = '%s' AND interval = '%s'
    '''%(self._table, name, interval)
    if i_end :
      stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
    else:
      stmt += ' AND i_time = %s'%(i_bucket)
    stmt += ' ORDER BY interval, i_time, r_time'

    cursor = connection.cursor()
    try:
      cursor.execute(stmt)
      for row in cursor:
        i_time, r_time, value, count = row
        if r_time==-1:
          r_time = None
        rval.setdefault(i_time,OrderedDict()).setdefault(r_time,{})[value] = count
    finally:
      cursor.close()
    return rval

class CassandraCount(CassandraBackend, Count):
  
  def __init__(self, *a, **kwargs):
    self._table = 'count'
    super(CassandraCount,self).__init__(*a, **kwargs)

    # TODO: use varint for [ir]_time?
    # TODO: support other value types
    cursor = self._client.cursor()
    try:
      res = cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        name text,
        interval text,
        i_time bigint,
        r_time bigint,
        count counter,
        PRIMARY KEY(name, interval, i_time, r_time)
      )'''%(self._table))
    except cql.ProgrammingError as pe:
      if 'existing' not in str(pe):
        raise
    finally:
      cursor.close()
  
  @scoped_connection
  def _insert_data(self, connection, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = -1
   
    # TODO: figure out escaping rules of CQL
    cursor = connection.cursor()
    try:
      table_spec = self._table
      expire = config['expire']
      if expire:
        table_spec += " USING TTL %s "%(expire)
      stmt = '''UPDATE %s SET count = count + %s
        WHERE name = '%s'
        AND interval = '%s'
        AND i_time = %s
        AND r_time = %s'''%(table_spec, value, name, interval, i_time, r_time)
      cursor.execute(stmt)
    finally:
      cursor.close()

  @scoped_connection
  def _type_get(self, connection, name, interval, i_bucket, i_end=None):
    rval = OrderedDict()
    
    # TODO: more efficient creation of query string
    stmt = '''SELECT i_time, r_time, count
      FROM %s
      WHERE name = '%s' AND interval = '%s'
    '''%(self._table, name, interval)
    if i_end :
      stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
    else:
      stmt += ' AND i_time = %s'%(i_bucket)
    stmt += ' ORDER BY interval, i_time, r_time'

    cursor = connection.cursor()
    try:
      cursor.execute(stmt)
      for row in cursor:
        i_time, r_time, count = row
        if r_time==-1:
          r_time = None
        rval.setdefault(i_time,OrderedDict())[r_time] = count
    finally:
      cursor.close()
    return rval

class CassandraGauge(CassandraBackend, Gauge):
  
  def __init__(self, *a, **kwargs):
    self._table = 'gauge'
    super(CassandraGauge,self).__init__(*a, **kwargs)

    # TODO: use varint for [ir]_time?
    # TODO: support other value types
    cursor = self._client.cursor()
    try:
      res = cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        name text,
        interval text,
        i_time bigint,
        r_time bigint,
        value %s,
        PRIMARY KEY(name, interval, i_time, r_time)
      )'''%(self._table, self._value_type))
    except cql.ProgrammingError as pe:
      if 'existing' not in str(pe):
        raise
    finally:
      cursor.close()
  
  @scoped_connection
  def _insert_data(self, connection, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = -1
   
    # TODO: figure out escaping rules of CQL
    cursor = connection.cursor()
    try:
      table_spec = self._table
      expire = config['expire']
      if expire:
        table_spec += " USING TTL %s "%(expire)
      stmt = '''UPDATE %s SET value = %s
        WHERE name = '%s'
        AND interval = '%s'
        AND i_time = %s
        AND r_time = %s'''%(table_spec, value, name, interval, i_time, r_time)
      cursor.execute(stmt)
    finally:
      cursor.close()

  @scoped_connection
  def _type_get(self, connection, name, interval, i_bucket, i_end=None):
    rval = OrderedDict()
    
    # TODO: more efficient creation of query string
    stmt = '''SELECT i_time, r_time, value
      FROM %s
      WHERE name = '%s' AND interval = '%s'
    '''%(self._table, name, interval)
    if i_end :
      stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
    else:
      stmt += ' AND i_time = %s'%(i_bucket)
    stmt += ' ORDER BY interval, i_time, r_time'

    cursor = connection.cursor()
    try:
      cursor.execute(stmt)
      for row in cursor:
        i_time, r_time, value = row
        if r_time==-1:
          r_time = None
        rval.setdefault(i_time,OrderedDict())[r_time] = value
    finally:
      cursor.close()
    return rval

class CassandraSet(CassandraBackend, Set):
  
  def __init__(self, *a, **kwargs):
    self._table = 'sets'
    super(CassandraSet,self).__init__(*a, **kwargs)

    # TODO: use varint for [ir]_time?
    # TODO: support other value types
    cursor = self._client.cursor()
    try:
      res = cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        name text,
        interval text,
        i_time bigint,
        r_time bigint,
        value %s,
        PRIMARY KEY(name, interval, i_time, r_time, value)
      )'''%(self._table, self._value_type))
    except cql.ProgrammingError as pe:
      if 'existing' not in str(pe):
        raise
    finally:
      cursor.close()
  
  @scoped_connection
  def _insert_data(self, connection, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = -1
   
    # TODO: figure out escaping rules of CQL
    cursor = connection.cursor()
    try:
      stmt = '''INSERT INTO %s (name, interval, i_time, r_time, value)
        VALUES ('%s', '%s', %s, %s, %s)'''%(self._table, name, interval, i_time, r_time, value)
      expire = config['expire']
      if expire:
        stmt += " USING TTL %s"%(expire)
      cursor.execute(stmt)
    finally:
      cursor.close()

  @scoped_connection
  def _type_get(self, connection, name, interval, i_bucket, i_end=None):
    rval = OrderedDict()
   
    # TODO: more efficient creation of query string
    stmt = '''SELECT i_time, r_time, value
      FROM %s
      WHERE name = '%s' AND interval = '%s'
    '''%(self._table, name, interval)
    if i_end :
      stmt += ' AND i_time >= %s AND i_time <= %s'%(i_bucket, i_end)
    else:
      stmt += ' AND i_time = %s'%(i_bucket)
    stmt += ' ORDER BY interval, i_time, r_time'

    cursor = connection.cursor()
    try:
      cursor.execute(stmt)
      for row in cursor:
        i_time, r_time, value = row
        if r_time==-1:
          r_time = None
        rval.setdefault(i_time,OrderedDict()).setdefault(r_time,set()).add( value )
    finally:
      cursor.close()
    return rval
