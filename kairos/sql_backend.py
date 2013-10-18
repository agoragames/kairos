'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from .exceptions import *
from .timeseries import *

from sqlalchemy import Table, Column, Integer, String, Float, MetaData, UniqueConstraint
from sqlalchemy.sql import select, update, insert, and_, or_, not_

import time

class SqlBackend(Timeseries):
  
  def __new__(cls, *args, **kwargs):
    if cls==SqlBackend:
      ttype = kwargs.pop('type', None)
      if ttype=='series':
        return SqlSeries.__new__(SqlSeries, *args, **kwargs)
      elif ttype=='histogram':
        return SqlHistogram.__new__(SqlHistogram, *args, **kwargs)
      elif ttype=='count':
        return SqlCount.__new__(SqlCount, *args, **kwargs)
      elif ttype=='gauge':
        return SqlGauge.__new__(SqlGauge, *args, **kwargs)
    return Timeseries.__new__(cls, *args, **kwargs)

  def __init__(self, client, **kwargs):
    '''
    Initialize the sql backend after timeseries has processed the configuration.
    '''
    self._metadata = MetaData()
    super(SqlBackend,self).__init__(client, **kwargs)

  def _insert(self, name, value, timestamp, intervals):
    '''
    Insert the new value.
    '''
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
  
  def _get(self, name, interval, config, timestamp, **kws):
    '''
    Get the interval.
    '''
    i_bucket = config['i_calc'].to_bucket(timestamp)
    fetch = kws.get('fetch')
    process_row = kws.get('process_row') or self._process_row

    rval = OrderedDict()
    if fetch:
      data = fetch( self._client.connect(), self._table, name, interval, i_bucket )
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

  def _series(self, name, interval, config, buckets, **kws):
    '''
    Fetch a series of buckets.
    '''
    fetch = kws.get('fetch')
    process_row = kws.get('process_row') or self._process_row

    rval = OrderedDict()

    if fetch:
      data = fetch( self._client.connect(), self._table, name, interval, buckets[0], buckets[-1] )
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

  def delete(self, name):
    '''
    Delete time series by name across all intervals. Returns the number of
    records deleted.
    '''
    conn = self._client.connect()
    conn.execute( self._table.delete().where(self._table.c.name==name) )

class SqlSeries(SqlBackend, Series):
  
  def __init__(self, *a, **kwargs):
    # TODO: Support kwargs so that we support more than just floats
    # TODO: define indices
    # TODO: optionally create separate tables for each interval, like mongo?
    super(SqlSeries,self).__init__(*a, **kwargs)
    self._table = Table('series', self._metadata,
      Column('name', String, nullable=False),       # stat name
      Column('interval', String, nullable=False),   # interval name
      Column('insert_time', Float, nullable=False), # to preserve order
      Column('i_time', Integer, nullable=False),    # interval timestamp
      Column('r_time', Integer, nullable=True),     # resolution timestamp
      Column('value', Float, nullable=False)        # datas
    )
    self._metadata.create_all(self._client)
  
  def _insert_data(self, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    kwargs = {
      'name'        : name,
      'interval'    : interval,
      'insert_time' : time.time(),
      'i_time'      : config['i_calc'].to_bucket(timestamp),
      'value'       : value
    }
    if not config['coarse']:
      kwargs['r_time'] = config['r_calc'].to_bucket(timestamp)
    stmt = self._table.insert().values(**kwargs)
    conn = self._client.connect()
    result = conn.execute(stmt)

  def _type_get(self, name, interval, i_bucket, i_end=None):
    connection = self._client.connect()
    rval = OrderedDict()
    stmt = self._table.select()

    if i_end:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time>=i_bucket,
          self._table.c.i_time<=i_end,
        )
      )
    else:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time==i_bucket,
        )
      )
    stmt = stmt.order_by( self._table.c.r_time, self._table.c.insert_time )

    for row in connection.execute(stmt):
      rval.setdefault(row['i_time'],OrderedDict()).setdefault(row['r_time'],[]).append( row['value'] )
    return rval

class SqlHistogram(SqlBackend, Histogram):
  
  def __init__(self, *a, **kwargs):
    # TODO: Support kwargs so that we support more than just floats
    # TODO: define indices
    # TODO: optionally create separate tables for each interval, like mongo?
    super(SqlHistogram,self).__init__(*a, **kwargs)
    self._table = Table('histogram', self._metadata,
      Column('name', String, nullable=False),       # stat name
      Column('interval', String, nullable=False),   # interval name
      Column('i_time', Integer, nullable=False),    # interval timestamp
      Column('r_time', Integer, nullable=True),     # resolution timestamp
      Column('value', Float, nullable=False),       # histogram keys
      Column('count', Integer, nullable=False),     # key counts

      # Use a constraint for transaction-less insert vs update
      UniqueConstraint('name', 'interval', 'i_time', 'r_time', 'value', name='unique_value')
    )
    self._metadata.create_all(self._client)
  
  def _insert_data(self, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    conn = self._client.connect()
    if not self._update_data(name, value, timestamp, interval, config, conn):
      try:
        kwargs = {
          'name'        : name,
          'interval'    : interval,
          'i_time'      : config['i_calc'].to_bucket(timestamp),
          'value'       : value,
          'count'       : 1
        }
        if not config['coarse']:
          kwargs['r_time'] = config['r_calc'].to_bucket(timestamp)
        stmt = self._table.insert().values(**kwargs)
        result = conn.execute(stmt)
      except:
        # TODO: only catch IntegrityError
        if not self._update_data(name, value, timestamp, interval, config, conn):
          raise

  def _update_data(self, name, value, timestamp, interval, config, conn):
    '''Support function for insert. Should be called within a transaction'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = None
    stmt = self._table.update().where(
      and_(
        self._table.c.name==name,
        self._table.c.interval==interval,
        self._table.c.i_time==i_time,
        self._table.c.r_time==r_time,
        self._table.c.value==value)
    ).values({self._table.c.count: self._table.c.count + 1})
    rval = conn.execute( stmt )
    return rval.rowcount

  def _type_get(self, name, interval, i_bucket, i_end=None):
    connection = self._client.connect()
    rval = OrderedDict()
    stmt = self._table.select()

    if i_end:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time>=i_bucket,
          self._table.c.i_time<=i_end,
        )
      )
    else:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time==i_bucket,
        )
      )
    stmt = stmt.order_by( self._table.c.r_time )

    for row in connection.execute(stmt):
      rval.setdefault(row['i_time'],OrderedDict()).setdefault(row['r_time'],{})[row['value']] = row['count']
    return rval

class SqlCount(SqlBackend, Count):
  
  def __init__(self, *a, **kwargs):
    # TODO: Support kwargs so that we support more than just floats
    # TODO: define indices
    # TODO: optionally create separate tables for each interval, like mongo?
    super(SqlCount,self).__init__(*a, **kwargs)
    self._table = Table('count', self._metadata,
      Column('name', String, nullable=False),       # stat name
      Column('interval', String, nullable=False),   # interval name
      Column('i_time', Integer, nullable=False),    # interval timestamp
      Column('r_time', Integer, nullable=True),     # resolution timestamp
      Column('count', Integer, nullable=False),     # key counts

      # Use a constraint for transaction-less insert vs update
      UniqueConstraint('name', 'interval', 'i_time', 'r_time', name='unique_count')
    )
    self._metadata.create_all(self._client)
  
  def _insert_data(self, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    conn = self._client.connect()
    if not self._update_data(name, value, timestamp, interval, config, conn):
      try:
        kwargs = {
          'name'        : name,
          'interval'    : interval,
          'i_time'      : config['i_calc'].to_bucket(timestamp),
          'count'       : value
        }
        if not config['coarse']:
          kwargs['r_time'] = config['r_calc'].to_bucket(timestamp)
        stmt = self._table.insert().values(**kwargs)
        result = conn.execute(stmt)
      except:
        # TODO: only catch IntegrityError
        if not self._update_data(name, value, timestamp, interval, config, conn):
          raise

  def _update_data(self, name, value, timestamp, interval, config, conn):
    '''Support function for insert. Should be called within a transaction'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = None
    stmt = self._table.update().where(
      and_(
        self._table.c.name==name,
        self._table.c.interval==interval,
        self._table.c.i_time==i_time,
        self._table.c.r_time==r_time)
    ).values({self._table.c.count: self._table.c.count + value})
    rval = conn.execute( stmt )
    return rval.rowcount

  def _type_get(self, name, interval, i_bucket, i_end=None):
    connection = self._client.connect()
    rval = OrderedDict()
    stmt = self._table.select()

    if i_end:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time>=i_bucket,
          self._table.c.i_time<=i_end,
        )
      )
    else:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time==i_bucket,
        )
      )
    stmt = stmt.order_by( self._table.c.r_time )

    for row in connection.execute(stmt):
      rval.setdefault(row['i_time'],OrderedDict())[row['r_time']] = row['count']
    return rval

class SqlGauge(SqlBackend, Gauge):
  
  def __init__(self, *a, **kwargs):
    # TODO: Support kwargs so that we support more than just floats
    # TODO: define indices
    # TODO: optionally create separate tables for each interval, like mongo?
    super(SqlGauge,self).__init__(*a, **kwargs)
    self._table = Table('gauge', self._metadata,
      Column('name', String, nullable=False),       # stat name
      Column('interval', String, nullable=False),   # interval name
      Column('i_time', Integer, nullable=False),    # interval timestamp
      Column('r_time', Integer, nullable=True),     # resolution timestamp
      Column('value', Float, nullable=False),     # key counts

      # Use a constraint for transaction-less insert vs update
      UniqueConstraint('name', 'interval', 'i_time', 'r_time', name='unique_count')
    )
    self._metadata.create_all(self._client)
  
  def _insert_data(self, name, value, timestamp, interval, config):
    '''Helper to insert data into sql.'''
    conn = self._client.connect()
    if not self._update_data(name, value, timestamp, interval, config, conn):
      try:
        kwargs = {
          'name'        : name,
          'interval'    : interval,
          'i_time'      : config['i_calc'].to_bucket(timestamp),
          'value'       : value
        }
        if not config['coarse']:
          kwargs['r_time'] = config['r_calc'].to_bucket(timestamp)
        stmt = self._table.insert().values(**kwargs)
        result = conn.execute(stmt)
      except:
        # TODO: only catch IntegrityError
        if not self._update_data(name, value, timestamp, interval, config, conn):
          raise

  def _update_data(self, name, value, timestamp, interval, config, conn):
    '''Support function for insert. Should be called within a transaction'''
    i_time = config['i_calc'].to_bucket(timestamp)
    if not config['coarse']:
      r_time = config['r_calc'].to_bucket(timestamp)
    else:
      r_time = None
    stmt = self._table.update().where(
      and_(
        self._table.c.name==name,
        self._table.c.interval==interval,
        self._table.c.i_time==i_time,
        self._table.c.r_time==r_time)
    ).values({self._table.c.value: value})
    rval = conn.execute( stmt )
    return rval.rowcount

  def _type_get(self, name, interval, i_bucket, i_end=None):
    connection = self._client.connect()
    rval = OrderedDict()
    stmt = self._table.select()

    if i_end:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time>=i_bucket,
          self._table.c.i_time<=i_end,
        )
      )
    else:
      stmt = stmt.where(
        and_(
          self._table.c.name==name,
          self._table.c.interval==interval,
          self._table.c.i_time==i_bucket,
        )
      )
    stmt = stmt.order_by( self._table.c.r_time )

    for row in connection.execute(stmt):
      rval.setdefault(row['i_time'],OrderedDict())[row['r_time']] = row['value']
    return rval
