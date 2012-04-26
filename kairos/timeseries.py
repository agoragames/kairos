'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from collections import OrderedDict
import operator
import time

class Timeseries(object):
  '''
  A time series object provides the interface to manage data sets in redis
  '''

  def __init__(self, client, config, key_prefix=''):
    '''
    Create a time series using a given redis client and configuration. 
    Optionally provide a prefix for all keys. If prefix length>0 and it
    doesn't end with ":", it will be automatically appended.

    The redis client must be API compatible with the Redis instance from
    the redis package http://pypi.python.org/pypi/redis

    The configuration must be a dictionary of the following form:

    {
      # interval name, used in redis keys and should conform to best practices
      # and not include ":"
      minute: {
        
        # Required. The number of seconds that the interval will cover
        step: 60,

        # Optional. The maximum number of intervals to maintain. If supplied,
        # will use redis expiration to delete old intervals, else intervals
        # exist in perpetuity.
        steps: 240,

        # Optional. Defines the resolution of the data, i.e. the number of 
        # seconds in which data is assumed to have occurred "at the same time".
        # So if you're tracking a month long time series, you may only need 
        # resolution down to the day, or resolution=86400. Defaults to same
        # value as "step".
        resolution: 60,

        # Optional. Rather than store time series of data, store only the
        # number of times a value was recorded in the interval. Defaults to
        # False.
        count_only = False,

        # Optional. If supplied, will cast all values read back with this
        # function or constructor. Does not apply if count_only==True.
        read_cast: float
      },
    }
    '''
    self._client = client
    self._config = config
    self._prefix = key_prefix
    if len(self._prefix) and not self._prefix.endswith(':'):
      self._prefix += ':'

  def insert(self, name, value, timestamp=None):
    '''
    Insert a value for the timeseries "name". For each interval in the 
    configuration, will insert the value into a bucket for the interval
    "timestamp". If time is not supplied, will default to time.time(), else it
    should be a floating point value.
    '''
    if not timestamp:
      timestamp = time.time()

    # TODO: document acceptable names
    # TODO: document what types values are supported
    # TODO: document behavior when time is outside the bounds of step*steps
    # TODO: support histograms
    # TODO: document how the data is stored.
    
    # Use pipe to reduce overhead
    pipe = self._client.pipeline()

    for interval,config in self._config.iteritems():
      step = config.get('step', 1)
      steps = config.get('steps',None)
      resolution = config.get('resolution',step)

      interval_bucket = int( timestamp / step )
      resolution_bucket = int( timestamp / resolution )

      interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)
      resolution_key = '%s:%s'%(interval_key, resolution_bucket)
      
      # If resolution is the same as step, store in the same row.
      if resolution==step:
        if config.get('count_only',False):
          pipe.incr(interval_key)
        elif config.get('compress', False):
          pipe.hincrby(interval_key, value, 1)
        else:
          pipe.rpush(interval_key, value)
      else:
        # Add the resolution bucket to the interval
        pipe.sadd(interval_key, resolution_bucket)

        # Figure out what we're storing.
        if config.get('count_only',False):
          pipe.incr(resolution_key)
        elif config.get('compress', False):
          pipe.hincrby(resolution_key, value, 1)
        else:
          pipe.rpush(resolution_key, value)

      if steps:
        pipe.expire(interval_key, step*steps)
        pipe.expire(resolution_key, step*steps)

    pipe.execute()

  def get(self, name, interval, timestamp=None, condensed=False):
    '''
    Get the set of values for a named timeseries and interval. If timestamp
    supplied, will fetch data for the period of time in which that timestamp
    would have fallen, else returns data for "now". If the timeseries 
    resolution was not defined, then returns a simple list of values for the
    interval, else returns an ordered dict where the keys define the resolution 
    interval and the values are the time series data in that (sub)interval. 
    This allows the user to interpolate sparse data sets.

    If the interval is count_only, values are cast to ints.
    '''
    # TODO: support negative values of timestamp as "-N intervals", i.e.
    # -1 on a day interval is yesterday
    if not timestamp:
      timestamp = time.time()

    config = self._config[interval]
    step = config.get('step', 1)
    resolution = config.get('resolution',step)

    interval_bucket = int( timestamp / step )
    interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

    rval = OrderedDict()    
    if resolution==step:
      if config.get('count_only',False):
        data = int( self._client.get(interval_key) )
      elif config.get('compress', False):
        data = self._client.hgetall(interval_key)
        # Turn back into a time series
        # TODO: this might be too slow because of the addition
        data = reduce(lambda res, (key,val): res + int(val)*[key], data.iteritems(), [] )
        if config.get('read_cast'):
          data = map(config.get('read_cast'), data)
      else:
        data = self._client.lrange(interval_key, 0, -1)
        if config.get('read_cast'):
          data = map(config.get('read_cast'), data)
      rval[ interval_bucket*step ] = data
    else:
      # First fetch all of the resolution buckets for this set.
      resolution_buckets = sorted(map(int,self._client.smembers(interval_key)))

      # Create a pipe and go fetch all the data for each.
      pipe = self._client.pipeline()
      for bucket in resolution_buckets:
        resolution_key = '%s:%s'%(interval_key, bucket)
        
        if config.get('count_only',False):
          pipe.get(resolution_key)
        elif config.get('compress', False):
          pipe.hgetall(resolution_key)
        else:
          pipe.lrange(resolution_key, 0, -1)
      
      res = pipe.execute()
      for idx,data in enumerate(res):
        if config.get('count_only',False):
          data = int(data) if data else 0
        elif config.get('compress', False):
          # Turn back into a time series
          # TODO: this might be too slow because of the addition
          data = reduce(lambda res, (key,val): res + int(val)*[key], data.iteritems(), [] )
          if config.get('read_cast'):
            data = map(config.get('read_cast'), data)
        elif config.get('read_cast'):
          data = map(config.get('read_cast'), data)
        
        rval[ resolution_buckets[idx]*resolution ] = data
    
    # If condensed, collapse the result into a single row
    if condensed:
      return reduce(operator.add, rval.values())
    return rval

  def count(self, name, interval, timestamp=None, condensed=False):
    '''
    Return a count of the number of datapoints for a time interval.

    Returns an ordered dictionary like get(), but the values are integers
    rather than lists.
    '''
    if not timestamp:
      timestamp = time.time()

    config = self._config[interval]
    step = config.get('step', 1)
    resolution = config.get('resolution',step)

    interval_bucket = int( timestamp / step )
    interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

    rval = OrderedDict()    
    if resolution==step:
      if config.get('count_only',False):
        data = int( self._client.get(interval_key) )
      elif config.get('compress', False):
        data = sum( map(int, self._client.hvals(interval_key)) )
      else:
        data = int( self._client.llen(interval_key) )
      rval[ interval_bucket*step ] = data
    else:
      # First fetch all of the resolution buckets for this set.
      resolution_buckets = sorted(map(int,self._client.smembers(interval_key)))

      # Create a pipe and go fetch all the data for each.
      pipe = self._client.pipeline()
      for bucket in resolution_buckets:
        resolution_key = '%s:%s'%(interval_key, bucket)
        
        if config.get('count_only',False):
          pipe.get(resolution_key)
        elif config.get('compress', False):
          pipe.hvals(resolution_key)
        else:
          pipe.llen(resolution_key)
      
      res = pipe.execute()
      for idx,data in enumerate(res):
        if config.get('compress', False):
          rval[ resolution_buckets[idx]*resolution ] = sum(map(int,data)) if data else 0
        else:
          rval[ resolution_buckets[idx]*resolution ] = int(data) if data else 0
    
    # If condensed, collapse the result into a single sum
    if condensed:
      return sum(rval.values())
      
    return rval

  def series(self, name, interval, steps=None, condensed=False):
    '''
    Return all the data in a named time series for a given interval. If steps
    not defined and there are none in the config, defaults to 1.

    Returns an ordered dict of interval timestamps to a single interval, which
    matches the return value in get().
    '''
    # TODO: support start and end timestamps
    config = self._config[interval]
    step = config.get('step', 1)
    steps = steps if steps else config.get('steps',1)
    resolution = config.get('resolution',step)

    end_timestamp = time.time()
    end_bucket = int( end_timestamp / step )
    start_bucket = end_bucket - steps +1 # +1 because it's inclusive of end

    # TODO: this isn't taking into account when step==resolution
    # Use pipe to reduce overhead
    pipe = self._client.pipeline()
    rval = OrderedDict()
    for s in range(steps):
      interval_bucket = start_bucket + s
      interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)
      rval[interval_bucket*step] = OrderedDict()

      if step==resolution:
        if config.get('count_only',False):
          pipe.get(interval_key)
        elif config.get('compress',False):
          pipe.hgetall(interval_key)
        else:
          pipe.lrange(interval_key, 0, -1)
      else:
        pipe.smembers(interval_key)
    res = pipe.execute()

    # TODO: a memory efficient way to use a single pipeline for this.
    for idx,data in enumerate(res):
      # Create a pipe and go fetch all the data for each.
      pipe = self._client.pipeline()
      interval_bucket = start_bucket + idx
      interval_key = '%s%s:%s:%s'%(self._prefix, name, interval, interval_bucket)

      if step==resolution:
        if config.get('count_only',False):
          data = int(data) if data else 0
        elif config.get('compress',False):
          # Turn back into a time series
          # TODO: this might be too slow because of the addition
          data = reduce(lambda res, (key,val): res + int(val)*[key], data.iteritems(), [] )
          if config.get('read_cast'):
            data = map(config.get('read_cast'), data)
        elif config.get('read_cast'):
          data = map(config.get('read_cast'), data)
        rval[interval_bucket*step] = data

      else:
        resolution_buckets = sorted(map(int,data))
        for bucket in resolution_buckets:
          resolution_key = '%s:%s'%(interval_key, bucket)
          
          if config.get('count_only',False):
            pipe.get(resolution_key)
          elif config.get('compress',False):
            pipe.hgetall(resolution_key)
          else:
            pipe.lrange(resolution_key, 0, -1)
        
        resolution_res = pipe.execute()
        for x,data in enumerate(resolution_res):
          if config.get('count_only',False):
            data = int(data) if data else 0
          elif config.get('compress',False):
            # Turn back into a time series
            # TODO: this might be too slow because of the addition
            data = reduce(lambda res, (key,val): res + int(val)*[key], data.iteritems(), [] )
            if config.get('read_cast'):
              data = map(config.get('read_cast'), data)
          elif config.get('read_cast'):
            data = map(config.get('read_cast'), data)
          
          rval[interval_bucket*step][ resolution_buckets[x]*resolution ] = data

    # If condensed, collapse each interval into a single value
    if condensed:
      for key in rval.keys():
        if config.get('count_only',False):
          rval[key] = sum(rval[key].values())
        else:
          rval[key] = reduce(operator.add, rval[key].values(), [])
    
    return rval
