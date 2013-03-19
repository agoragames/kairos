'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from exceptions import *

import operator
import sys
import time
import re

from timeseries import *

class RedisBackend(Timeseries):
  '''
  Redis implementation of timeseries support.
  '''

  def __new__(cls, *args, **kwargs):
    if cls==RedisBackend:
      ttype = kwargs.pop('type', None)
      if ttype=='series':
        return RedisSeries.__new__(RedisSeries, *args, **kwargs)
      elif ttype=='histogram':
        return RedisHistogram.__new__(RedisHistogram, *args, **kwargs)
      elif ttype=='count':
        return RedisCount.__new__(RedisCount, *args, **kwargs)
      elif ttype=='gauge':
        return RedisGauge.__new__(RedisGauge, *args, **kwargs)
    return Timeseries.__new__(cls, *args, **kwargs)

  def __init__(self, client, **kwargs):
    super(RedisBackend,self).__init__( client, **kwargs )

    # prefix is redis-only feature (TODO: yes or no?)
    self._prefix = kwargs.get('prefix', '')
    if len(self._prefix) and not self._prefix.endswith(':'):
      self._prefix += ':'

  def _insert(self, name, value, timestamp):
    '''
    Insert the value.
    '''
    pipe = self._client.pipeline(transaction=False)
    # TODO: apply the prefix if we're using one.

    for interval,config in self._intervals.iteritems():
      i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)
      
      if config['coarse']:
        self._type_insert(pipe, i_key, value)
      else:
        # Add the resolution bucket to the interval. This allows us to easily
        # discover the resolution intervals within the larger interval, and
        # if there is a cap on the number of steps, it will go out of scope
        # along with the rest of the data
        pipe.sadd(i_key, r_bucket)
        self._type_insert(pipe, r_key, value)

      expire = config['expire']
      if expire:
        pipe.expire(i_key, expire)
        if not config['coarse']:
          pipe.expire(r_key, expire)

    pipe.execute()

  def delete(self, name):
    '''
    Delete all the data in a named timeseries.
    '''
    keys = self._client.keys('%s%s:*'%(self._prefix,name))

    pipe = self._client.pipeline(transaction=False)
    for key in keys:
      pipe.delete( key )
    pipe.execute()

    # Could be not technically the exact number of keys deleted, but is a close
    # enough approximation
    return len(keys)

  def _get(self, name, interval, config, timestamp):
    '''
    Fetch a single interval from redis.
    '''
    i_bucket, r_bucket, i_key, r_key = config['calc_keys'](name, timestamp)
    
    rval = OrderedDict()    
    if config['coarse']:
      data = self._process_row( self._type_get(self._client, i_key) )
      rval[ i_bucket*config['step'] ] = data
    else:
      # First fetch all of the resolution buckets for this set.
      resolution_buckets = sorted(map(int,self._client.smembers(i_key)))

      # Create a pipe and go fetch all the data for each.
      # TODO: turn off transactions here?
      pipe = self._client.pipeline(transaction=False)
      for bucket in resolution_buckets:
        r_key = '%s:%s'%(i_key, bucket)   # TODO: make this the "resolution_bucket" closure?
        self._type_get(pipe, r_key)
      res = pipe.execute()

      for idx,data in enumerate(res):
        data = self._process_row(data)
        rval[ resolution_buckets[idx]*config['resolution'] ] = data

    return rval

  def _series(self, name, interval, config, buckets):
    '''
    Fetch a series of buckets.
    '''
    pipe = self._client.pipeline(transaction=False)
    step = config['step']
    resolution = config.get('resolution',step)

    rval = OrderedDict()
    for interval_bucket in buckets:
      i_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

      if config['coarse']:
        self._type_get(pipe, i_key)
      else:
        pipe.smembers(i_key)
    res = pipe.execute()

    # TODO: a memory efficient way to use a single pipeline for this.
    for idx,data in enumerate(res):
      # TODO: use closures on the config for generating this interval key
      interval_bucket = buckets[idx] #start_bucket + idx
      interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

      if config['coarse']:
        data = self._process_row( data )
        rval[interval_bucket*step] = data
      else:
        rval[interval_bucket*step] = OrderedDict()
        pipe = self._client.pipeline(transaction=False)
        resolution_buckets = sorted(map(int,data))
        for bucket in resolution_buckets:
          # TODO: use closures on the config for generating this resolution key
          resolution_key = '%s:%s'%(interval_key, bucket)
          self._type_get(pipe, resolution_key)
        
        resolution_res = pipe.execute()
        for x,data in enumerate(resolution_res):
          rval[interval_bucket*step][ resolution_buckets[x]*resolution ] = \
            self._process_row(data)

    return rval

class RedisSeries(RedisBackend, Series):

  def _type_insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.rpush(key, value)

  def _type_get(self, handle, key):
    '''
    Get for a series.
    '''
    return handle.lrange(key, 0, -1)

class RedisHistogram(RedisBackend, Histogram):

  def _type_insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.hincrby(key, value, 1)

  def _type_get(self, handle, key):
    return handle.hgetall(key)

class RedisCount(RedisBackend, Count):

  def _type_insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    if value!=0:
      if isinstance(value,float):
        handle.incrbyfloat(key, value)
      else:
        handle.incr(key,value)
  
  def _type_get(self, handle, key):
    return handle.get(key)

class RedisGauge(RedisBackend, Gauge):
  
  def _type_insert(self, handle, key, value):
    '''
    Insert the value into the series.
    '''
    handle.set(key, value)
  
  def _type_get(self, handle, key):
    return handle.get(key)
