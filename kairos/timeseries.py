'''
Copyright (c) 2012, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
import time

class Timeseries(object):
  '''
  A time series object provides the interface to manage data sets in redis
  '''

  def __init__(self, client, config):
    '''
    Create a time series using a given redis client and configuration.

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

        # Optional. If supplied, will cast all values read back with this
        # function or constructor.
        read_cast: float
      },
    }
    '''
    self._client = client
    self._config = config

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
    for interval,config in self._config.iteritems():
      bucket = int( timestamp / config['step'] )
      key = '%s:%s:%s'%(name, interval, bucket)
      self._client.rpush(key, value)
      if config.get('steps'):
        self._client.expire(key, config['step']*config['steps'])

  def get(self, name, interval, timestamp=None):
    '''
    Get the set of values for a named timeseries and interval. If timestamp
    supplied, will fetch data for the period of time in which that timestamp
    would have fallen, else returns data for "now".
    '''
    if not timestamp:
      timestamp = time.time()

    config = self._config[interval]
    bucket = int( timestamp / config['step'] )
    key = '%s:%s:%s'%(name, interval, bucket)

    rval = self._client.lrange(key, 0, -1)
    if config.get('read_cast'):
      rval = map(config.get('read_cast'), rval)
    return rval
