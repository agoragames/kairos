'''
Copyright (c) 2012-2017, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from .exceptions import *
from .timeseries import *

import operator
import sys
import time
import re
import pymongo
from pymongo import ASCENDING, DESCENDING
from datetime import datetime
from urllib.parse import *

class MongoBackend(Timeseries):
  '''
  Mongo implementation of timeseries support.
  '''

  def __new__(cls, *args, **kwargs):
    if cls==MongoBackend:
      ttype = kwargs.pop('type', None)
      if ttype=='series':
        return MongoSeries.__new__(MongoSeries, *args, **kwargs)
      elif ttype=='histogram':
        return MongoHistogram.__new__(MongoHistogram, *args, **kwargs)
      elif ttype=='count':
        return MongoCount.__new__(MongoCount, *args, **kwargs)
      elif ttype=='gauge':
        return MongoGauge.__new__(MongoGauge, *args, **kwargs)
      raise NotImplementedError("No implementation for %s types"%(ttype))
    return Timeseries.__new__(cls, *args, **kwargs)

  def __init__(self, client, **kwargs):
    '''
    Initialize the mongo backend after timeseries has processed the configuration.
    '''
    if isinstance(client, pymongo.MongoClient):
      client = client['kairos']
    elif not isinstance(client, pymongo.database.Database):
      raise TypeError('Mongo handle must be MongoClient or database instance')

    self._escape_character = kwargs.get('escape_character', "\U0000FFFF")
    super(MongoBackend,self).__init__(client, **kwargs)

    # Define the indices for lookups and TTLs
    for interval,config in list(self._intervals.items()):
      # TODO: the interval+(resolution+)name combination should be unique,
      # but should the index be defined as such? Consider performance vs.
      # correctness tradeoff. Also will need to determine if we need to
      # maintain this multikey index or if there's a better way to implement
      # all the features. Lastly, determine if it's better to add 'value'
      # to the index and spec the fields in get() and series() so that we
      # get covered indices. There are reasons why that might be a
      # configuration option (performance vs. memory tradeoff)
      if config['coarse']:
        self._client[interval].ensure_index(
          [('interval',ASCENDING),('name',ASCENDING)], background=True )
      else:
        self._client[interval].ensure_index(
          [('interval',ASCENDING),('resolution',ASCENDING),('name',ASCENDING)],
          background=True )
      if config['expire']:
        self._client[interval].ensure_index(
          [('expire_from',ASCENDING)], expireAfterSeconds=config['expire'], background=True )

  @classmethod
  def url_parse(self, url, **kwargs):
    location = urlparse(url)
    if location.scheme == 'mongodb':
      client = pymongo.MongoClient( url, **kwargs )

      # Stupid urlparse has a "does this scheme use queries" registrar,
      # so copy that work here. Then pull out the optional database name.
      path = location.path
      if '?' in path:
        path = path.split('?',1)[0]
      path = re.search('[/]*([\w]*)', path).groups()[0] or kwargs.get('database','kairos')

      return client[ path ]

  # A very ugly way to capture histogram updates
  @property
  def _single_value(self):
    return True

  def _unescape(self, value):
    '''
    Recursively unescape values. Though slower, this doesn't require the user to
    know anything about the escaping when writing their own custom fetch functions.
    '''
    if isinstance(value, str):
      return value.replace(self._escape_character, '.')
    elif isinstance(value, dict):
      return { self._unescape(k) : self._unescape(v) for k,v in list(value.items()) }
    elif isinstance(value, list):
      return [ self._unescape(v) for v in value ]
    return value

  def list(self):
    rval = set()
    for interval,config in list(self._intervals.items()):
      rval.update( self._client.command({'distinct':interval, 'key':'name'})['values'] )
    return list(rval)

  def properties(self, name):
    rval = {}
    for interval,config in list(self._intervals.items()):
      rval.setdefault(interval, {})
      query = {'name':name}
      res = self._client[interval].find_one(query, sort=[('interval',ASCENDING)])
      rval[interval]['first'] = config['i_calc'].from_bucket(res['interval'])
      res = self._client[interval].find_one(query, sort=[('interval',DESCENDING)])
      rval[interval]['last'] = config['i_calc'].from_bucket(res['interval'])

    return rval

  def _batch_key(self, query):
    '''
    Get a unique id from a query.
    '''
    return ''.join( ['%s%s'%(k,v) for k,v in sorted(query.items())] )

  def _batch_insert(self, inserts, intervals, **kwargs):
    '''
    Batch insert implementation.
    '''
    updates = {}
    # TODO support flush interval
    for interval,config in list(self._intervals.items()):
      for timestamp,names in inserts.items():
        timestamps = self._normalize_timestamps(timestamp, intervals, config)
        for name,values in names.items():
          for value in values:
            for tstamp in timestamps:
              query,insert = self._insert_data(
                name, value, tstamp, interval, config, dry_run=True)

              batch_key = self._batch_key(query)
              updates.setdefault(batch_key, {'query':query, 'interval':interval})
              new_insert = self._batch(insert, updates[batch_key].get('insert'))
              updates[batch_key]['insert'] = new_insert

    # now that we've collected a bunch of updates, flush them out
    for spec in list(updates.values()):
      self._client[ spec['interval'] ].update( 
        spec['query'], spec['insert'], upsert=True, check_keys=False )

  def _insert(self, name, value, timestamp, intervals, **kwargs):
    '''
    Insert the new value.
    '''
    # TODO: confirm that this is in fact using the indices correctly.
    for interval,config in list(self._intervals.items()):
      timestamps = self._normalize_timestamps(timestamp, intervals, config)
      for tstamp in timestamps:
        self._insert_data(name, value, tstamp, interval, config, **kwargs)

  def _insert_data(self, name, value, timestamp, interval, config, **kwargs):
    '''Helper to insert data into mongo.'''
    # Mongo does not allow mixing atomic modifiers and non-$set sets in the
    # same update, so the choice is to either run the first upsert on
    # {'_id':id} to ensure the record is in place followed by an atomic update
    # based on the series type, or use $set for all of the keys to be used in
    # creating the record, which then disallows setting our own _id because
    # that "can't be updated". So the tradeoffs are:
    #   * 2 updates for every insert, maybe a local cache of known _ids and the
    #     associated memory overhead
    #   * Query by the 2 or 3 key tuple for this interval and the associated
    #     overhead of that index match vs. the presumably-faster match on _id
    #   * Yet another index for the would-be _id of i_key or r_key, where each
    #     index has a notable impact on write performance.
    # For now, choosing to go with matching on the tuple until performance
    # testing can be done. Even then, there may be a variety of factors which
    # make the results situation-dependent.
    insert = {'name':name, 'interval':config['i_calc'].to_bucket(timestamp)}
    if not config['coarse']:
      insert['resolution'] = config['r_calc'].to_bucket(timestamp)
    # copy the query before expire_from as that is not indexed
    query = insert.copy()

    if config['expire']:
      insert['expire_from'] = datetime.utcfromtimestamp( timestamp )

    # switch to atomic updates
    insert = {'$set':insert.copy()}

    # need to hide the period of any values. best option seems to be to pick
    # a character that "no one" uses.
    if isinstance(value, str):
      value = value.replace('.', self._escape_character)
    elif isinstance(value, float):
      value = str(value).replace('.', self._escape_character)

    self._insert_type( insert, value )

    # TODO: use write preference settings if we have them
    if not kwargs.get('dry_run',False):
      self._client[interval].update( query, insert, upsert=True, check_keys=False )
    return query, insert

  def _get(self, name, interval, config, timestamp, **kws):
    '''
    Get the interval.
    '''
    i_bucket = config['i_calc'].to_bucket(timestamp)
    fetch = kws.get('fetch')
    process_row = kws.get('process_row') or self._process_row

    rval = OrderedDict()
    query = {'name':name, 'interval':i_bucket}
    if config['coarse']:
      if fetch:
        record = fetch( self._client[interval], spec=query, method='find_one' )
      else:
        record = self._client[interval].find_one( query )

      if record:
        data = process_row( self._unescape(record['value']) )
        rval[ config['i_calc'].from_bucket(i_bucket) ] = data
      else:
        rval[ config['i_calc'].from_bucket(i_bucket) ] = self._type_no_value()
    else:
      sort = [('interval', ASCENDING), ('resolution', ASCENDING) ]
      if fetch:
        cursor = fetch( self._client[interval], spec=query, sort=sort, method='find' )
      else:
        cursor = self._client[interval].find( spec=query, sort=sort )

      idx = 0
      for record in cursor:
        rval[ config['r_calc'].from_bucket(record['resolution']) ] = \
          process_row(record['value'])

    return rval

  def _series(self, name, interval, config, buckets, **kws):
    '''
    Fetch a series of buckets.
    '''
    # make a copy of the buckets because we're going to mutate it
    buckets = list(buckets)
    rval = OrderedDict()
    step = config['step']
    resolution = config.get('resolution',step)
    fetch = kws.get('fetch')
    process_row = kws.get('process_row', self._process_row)

    query = { 'name':name, 'interval':{'$gte':buckets[0], '$lte':buckets[-1]} }
    sort = [('interval', ASCENDING)]
    if not config['coarse']:
      sort.append( ('resolution', ASCENDING) )

    if fetch:
      cursor = fetch( self._client[interval], spec=query, sort=sort, method='find' )
    else:
      cursor = self._client[interval].find( spec=query, sort=sort )
    for record in cursor:
      while buckets and buckets[0] < record['interval']:
        rval[ config['i_calc'].from_bucket(buckets.pop(0)) ] = self._type_no_value()
      if buckets and buckets[0]==record['interval']:
        buckets.pop(0)

      i_key = config['i_calc'].from_bucket(record['interval'])
      data = process_row( record['value'] )
      if config['coarse']:
        rval[ i_key ] = data
      else:
        rval.setdefault( i_key, OrderedDict() )
        rval[ i_key ][ config['r_calc'].from_bucket(record['resolution']) ] = data

    # are there buckets at the end for which we received no data?
    while buckets:
      rval[ config['i_calc'].from_bucket(buckets.pop(0)) ] = self._type_no_value()

    return rval

  def delete(self, name):
    '''
    Delete time series by name across all intervals. Returns the number of
    records deleted.
    '''
    # TODO: confirm that this does not use the combo index and determine
    # performance implications.
    num_deleted = 0
    for interval,config in list(self._intervals.items()):
      # TODO: use write preference settings if we have them
      num_deleted += self._client[interval].remove( {'name':name} )['n']
    return num_deleted

class MongoSeries(MongoBackend, Series):

  def _batch(self, insert, existing):
    if not existing:
      insert['$push'] = {'value':{'$each':[ insert['$push']['value'] ]}}
      return insert
      
    existing['$push']['value']['$each'].append( insert.pop('$push')['value'] )
    return existing

  def _insert_type(self, spec, value):
    spec['$push'] = {'value':value}

class MongoHistogram(MongoBackend, Histogram):
  
  def _batch(self, insert, existing):
    if not existing:
      return insert

    for value,incr in insert['$inc'].items():
      existing['$inc'][value] = existing['$inc'].get(value,0)+incr
    return existing

  def _insert_type(self, spec, value):
    spec['$inc'] = {'value.%s'%(value): 1}

class MongoCount(MongoBackend, Count):
  
  def _batch(self, insert, existing):
    if not existing:
      return insert

    existing['$inc']['value'] += insert['$inc']['value']
    return existing

  def _insert_type(self, spec, value):
    spec['$inc'] = {'value':value}

class MongoGauge(MongoBackend, Gauge):

  def _batch(self, insert, existing):
    if not existing:
      return insert

    existing['$set']['value'] = insert['$set']['value']
    return existing

  def _insert_type(self, spec, value):
    spec['$set']['value'] = value
