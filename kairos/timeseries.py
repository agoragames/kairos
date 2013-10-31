'''
Copyright (c) 2012-2013, Agora Games, LLC All rights reserved.

https://github.com/agoragames/kairos/blob/master/LICENSE.txt
'''
from .exceptions import *

from datetime import datetime, timedelta
import operator
import sys
import time
import re

if sys.version_info[:2] > (2, 6):
    from collections import OrderedDict
else:
    from ordereddict import OrderedDict

from monthdelta import MonthDelta

BACKENDS = {}

NUMBER_TIME = re.compile('^[\d]+$')
SIMPLE_TIME = re.compile('^([\d]+)([hdwmy])$')

SIMPLE_TIMES = {
  'h' : 60*60,        # hour
  'd' : 60*60*24,     # day
  'w' : 60*60*24*7,   # week
  'm' : 60*60*24*30,  # month(-ish)
  'y' : 60*60*24*365, # year(-ish)
}

GREGORIAN_TIMES = set(['daily', 'weekly', 'monthly', 'yearly'])

# Test python3 compatibility
try:
  x = long(1)
except NameError:
  long = int

def _resolve_time(value):
  '''
  Resolve the time in seconds of a configuration value.
  '''
  if value is None or isinstance(value,(int,long)):
    return value

  if NUMBER_TIME.match(value):
    return long(value)

  simple = SIMPLE_TIME.match(value)
  if SIMPLE_TIME.match(value):
    multiplier = long( simple.groups()[0] )
    constant = SIMPLE_TIMES[ simple.groups()[1] ]
    return multiplier * constant

  if value in GREGORIAN_TIMES:
    return value

  raise ValueError('Unsupported time format %s'%value)

class RelativeTime(object):
  '''
  Functions associated with relative time intervals.
  '''

  def __init__(self, step=1):
    self._step = step

  def to_bucket(self, timestamp, steps=0):
    '''
    Calculate the bucket from a timestamp, optionally including a step offset.
    '''
    return int( timestamp / self._step ) + steps

  def from_bucket(self, bucket):
    '''
    Calculate the timestamp given a bucket.
    '''
    return bucket * self._step

  def buckets(self, start, end):
    '''
    Calculate the buckets within a starting and ending timestamp.
    '''
    start_bucket = self.to_bucket(start)
    end_bucket = self.to_bucket(end)
    return range(start_bucket, end_bucket+1) 

  def normalize(self, timestamp, steps=0):
    '''
    Normalize a timestamp according to the interval configuration. Can
    optionally determine an offset.
    '''
    return self.from_bucket( self.to_bucket(timestamp, steps) )

  def ttl(self, steps):
    '''
    Return the ttl given the number of steps, None if steps is not defined
    or we're otherwise unable to calculate one.
    '''
    if steps:
      return steps * self._step

    return None

class GregorianTime(object):
  '''
  Functions associated with gregorian time intervals.
  '''
  # NOTE: strptime weekly has the following bug:
  # In [10]: datetime.strptime('197001', '%Y%U')
  # Out[10]: datetime.datetime(1970, 1, 1, 0, 0)
  # In [11]: datetime.strptime('197002', '%Y%U')
  # Out[11]: datetime.datetime(1970, 1, 1, 0, 0)

  FORMATS = {
    'daily'   : '%Y%m%d',
    'weekly'  : '%Y%U',
    'monthly' : '%Y%m',
    'yearly'  : '%Y'
  }
  
  def __init__(self, step='daily'):
    self._step = step

  def to_bucket(self, timestamp, steps=0):
    '''
    Calculate the bucket from a timestamp.
    '''
    dt = datetime.utcfromtimestamp( timestamp )

    if steps!=0:
      if self._step == 'daily':
        dt = dt + timedelta(days=steps)
      elif self._step == 'weekly':
        dt = dt + timedelta(weeks=steps)
      elif self._step == 'monthly':
        dt = dt + MonthDelta(steps)
      elif self._step == 'yearly':
        year = int(dt.strftime( self.FORMATS[self._step] ))
        year += steps
        dt = datetime(year=year, month=1, day=1)

    return int(dt.strftime( self.FORMATS[self._step] ))

  def from_bucket(self, bucket):
    '''
    Calculate the timestamp given a bucket.
    '''
    # NOTE: this is due to a bug somewhere in strptime that does not process
    # the week number of '%Y%U' correctly. That bug could be very specific to
    # the combination of python and ubuntu that I was testing.
    bucket = str(bucket)
    if self._step == 'weekly':
      year, week = bucket[:4], bucket[4:]
      normal = datetime(year=int(year), month=1, day=1) + timedelta(weeks=int(week))
    else:
      normal = datetime.strptime(bucket, self.FORMATS[self._step])
    return long(time.mktime( normal.timetuple() ))

  def buckets(self, start, end):
    '''
    Calculate the buckets within a starting and ending timestamp.
    '''
    rval = [ self.to_bucket(start) ]
    step = 1

    # In theory there's already been a check that end>start
    # TODO: Not a fan of an unbound while loop here
    while True:
      bucket = self.to_bucket(start, step)
      bucket_time = self.from_bucket( bucket )
      if bucket_time >= end:
        if bucket_time==end:
          rval.append( bucket )
        break
      rval.append( bucket )
      step += 1

    return rval 

  def normalize(self, timestamp, steps=0):
    '''
    Normalize a timestamp according to the interval configuration. Optionally
    can be used to calculate the timestamp N steps away.
    '''
    # So far, the only commonality with RelativeTime
    return self.from_bucket( self.to_bucket(timestamp, steps) )

  def ttl(self, steps):
    '''
    Return the ttl given the number of steps, None if steps is not defined
    or we're otherwise unable to calculate one.
    '''
    if steps:
      # Approximate the ttl based on number of seconds, since it's 
      # "close enough" 
      return steps * SIMPLE_TIMES[ self._step[0] ]

    return None

class Timeseries(object):
  '''
  Base class of all time series. Also acts as a factory to return the correct
  subclass if "type=" keyword argument supplied.
  '''
  
  def __new__(cls, client, **kwargs):
    if cls==Timeseries:
      # load a backend based on the name of the client module 
      client_module = client.__module__.split('.')[0]
      backend = BACKENDS.get( client_module )
      if backend:
        return backend( client, **kwargs )
      else:
        raise ImportError("Unsupported or unknown client type %s", client_module)
    return object.__new__(cls, client, **kwargs)

  def __init__(self, client, **kwargs):
    '''
    Create a time series using a given redis client and keyword arguments
    defining the series configuration. 

    Optionally provide a prefix for all keys. If prefix length>0 and it
    doesn't end with ":", it will be automatically appended. Redis only.

    The redis client must be API compatible with the Redis instance from
    the redis package http://pypi.python.org/pypi/redis


    The supported keyword arguments are:

    type
      One of (series, histogram, count). Optional, defaults to "series".

      series - each interval will append values to a list
      histogram - each interval will track count of unique values
      count - each interval will maintain a single counter

    prefix
      Optional, is a prefix for all keys in this histogram. If supplied
      and it doesn't end with ":", it will be automatically appended.
      Redis only.

    read_func
      Optional, is a function applied to all values read back from the
      database. Without it, values will be strings. Must accept a string
      value and can return anything.

    write_func
      Optional, is a function applied to all values when writing. Can be
      used for histogram resolution, converting an object into an id, etc.
      Must accept whatever can be inserted into a timeseries and return an
      object which can be cast to a string.

    intervals
      Required, a dictionary of interval configurations in the form of: 

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
        }
      }
    '''
    # Process the configuration first so that the backends can use that to 
    # complete their setup.
    # Prefix is determined by the backend implementation.
    self._client = client
    self._read_func = kwargs.get('read_func',None)
    self._write_func = kwargs.get('write_func',None)
    self._intervals = kwargs.get('intervals', {})

    # Preprocess the intervals
    for interval,config in self._intervals.items():
      # Copy the interval name into the configuration, needed for redis
      config['interval'] = interval
      step = config['step'] = _resolve_time( config['step'] ) # Required
      steps = config.get('steps',None)       # Optional
      resolution = config['resolution'] = _resolve_time( 
        config.get('resolution',config['step']) ) # Optional

      if step in GREGORIAN_TIMES:
        interval_calc = GregorianTime(step)
      else:
        interval_calc = RelativeTime(step)

      if resolution in GREGORIAN_TIMES:
        resolution_calc = GregorianTime(resolution)
      else:
        resolution_calc = RelativeTime(resolution)
      
      expire = False
      if steps: expire = step*steps
      
      config['i_calc'] = interval_calc
      config['r_calc'] = resolution_calc
      
      config['expire'] = interval_calc.ttl( steps )
      config['coarse'] = (resolution==step)

  def list(self):
    '''
    List all of the stat names that are stored.
    '''
    raise NotImplementedError()

  def properties(self, name):
    '''
    Get the properties of a stat.
    '''
    raise NotImplementedError()

  def expire(self, name):
    '''
    Manually expire data for storage engines that do not support auto expiry.
    '''
    raise NotImplementedError()

  def insert(self, name, value, timestamp=None, intervals=0):
    '''
    Insert a value for the timeseries "name". For each interval in the 
    configuration, will insert the value into a bucket for the interval
    "timestamp". If time is not supplied, will default to time.time(), else it
    should be a floating point value.

    If "intervals" is less than 0, inserts the value into timestamps
    "abs(intervals)" preceeding "timestamp" (i.e. "-1" inserts one extra value).
    If "intervals" is greater than 0, inserts the value into that many more
    intervals after "timestamp". The default behavior is to insert for a single
    timestamp.

    This supports the public methods of the same name in the subclasses. The
    value is expected to already be converted.
    '''
    if not timestamp:
      timestamp = time.time()
    if self._write_func:
      value = self._write_func(value)

    # TODO: document acceptable names
    # TODO: document what types values are supported
    # TODO: document behavior when time is outside the bounds of TTLed config
    # TODO: document how the data is stored.
    # TODO: better abstraction for "intervals" processing rather than in each implementation

    self._insert( name, value, timestamp, intervals )

  def _insert(self, name, value, timestamp, intervals):
    '''
    Support for the insert per type of series.
    '''
    raise NotImplementedError()

  def delete(self, name):
    '''
    Delete all data in a timeseries. Subclasses are responsible for 
    implementing this.
    '''
    raise NotImplementedError()

  def delete_all(self):
    '''
    Deletes all data in every timeseries. Default implementation is to walk
    all of the names and call delete(stat), storage implementations are welcome
    to optimize this.
    '''
    for name in self.list():
      self.delete(name)

  def iterate(self, name, interval, **kwargs):
    '''
    Returns a generator that iterates over all the intervals and returns
    data for various timestamps, in the form:

      ( unix_timestamp, data )

    This will check for all timestamp buckets that might exist between
    the first and last timestamp in a series. Each timestamp bucket will
    be fetched separately to keep this memory efficient, at the cost of
    extra trips to the data store.

    Keyword arguments are the same as get().
    '''
    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)
    properties = self.properties(name)[interval]

    i_buckets = config['i_calc'].buckets(properties['first'], properties['last'])
    for i_bucket in i_buckets:
      data = self.get(name, interval,
        timestamp=config['i_calc'].from_bucket(i_bucket), **kwargs)
      for timestamp,row in data.items():
        yield (timestamp,row)

  def get(self, name, interval, **kwargs):
    '''
    Get the set of values for a named timeseries and interval. If timestamp
    supplied, will fetch data for the period of time in which that timestamp
    would have fallen, else returns data for "now". If the timeseries 
    resolution was not defined, then returns a simple list of values for the
    interval, else returns an ordered dict where the keys define the resolution 
    interval and the values are the time series data in that (sub)interval. 
    This allows the user to interpolate sparse data sets.

    If transform is defined, will utilize one of `[mean, count, min, max, sum]`
    to process each row of data returned. If the transform is a callable, will
    pass an array of data to the function. Note that the transform will be run
    after the data is condensed. If the transform is a list, then each row will
    return a hash of the form { transform_name_or_func : transformed_data }. 
    If the transform is a hash, then it should be of the form 
    { transform_name : transform_func } and will return the same structure as
    a list argument.

    Raises UnknownInterval if `interval` is not one of the configured 
    intervals.

    TODO: Fix this method doc
    '''
    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)

    timestamp = kwargs.get('timestamp', time.time())
    fetch = kwargs.get('fetch')
    process_row = kwargs.get('process_row') or self._process_row
    condense = kwargs.get('condense', False)
    join_rows = kwargs.get('join_rows') or self._join
    transform = kwargs.get('transform')

    # DEPRECATED handle the deprecated version of condense
    condense = kwargs.get('condensed',condense)

    # If name is a list, then join all of results. It is more efficient to
    # use a single data structure and join "in-line" but that requires a major
    # refactor of the backends, so trying this solution to start with. At a
    # minimum we'd have to rebuild the results anyway because of the potential
    # for sparse data points would result in an out-of-order result.
    if isinstance(name, (list,tuple,set)):
      results = [ self._get(x, interval, config, timestamp, fetch=fetch, process_row=process_row) for x in name ]
      # Even resolution data is "coarse" in that it's not nested
      rval = self._join_results( results, True, join_rows )
    else:
      rval = self._get( name, interval, config, timestamp, fetch=fetch, process_row=process_row )

    # If condensed, collapse the result into a single row
    if condense and not config['coarse']:
      condense = condense if callable(condense) else self._condense
      rval = { config['i_calc'].normalize(timestamp) : condense(rval) }
    if transform:
      for k,v in rval.items():
        rval[k] = self._process_transform(v, transform)
    return rval

  def _get(self, name, interval, config, timestamp, fetch):
    '''
    Support for the insert per type of series.
    '''
    raise NotImplementedError()
  
  def series(self, name, interval, **kwargs):
    '''
    Return all the data in a named time series for a given interval. If steps
    not defined and there are none in the config, defaults to 1.

    Returns an ordered dict of interval timestamps to a single interval, which
    matches the return value in get().

    If transform is defined, will utilize one of `[mean, count, min, max, sum]`
    to process each row of data returned. If the transform is a callable, will
    pass an array of data to the function. Note that the transform will be run
    after the data is condensed.

    Raises UnknownInterval if `interval` not configured.
    '''
    config = self._intervals.get(interval)
    if not config:
      raise UnknownInterval(interval)

    start = kwargs.get('start')
    end = kwargs.get('end')
    steps = kwargs.get('steps') or config.get('steps',1)

    fetch = kwargs.get('fetch')
    process_row = kwargs.get('process_row') or self._process_row
    condense = kwargs.get('condense', False)
    join_rows = kwargs.get('join_rows') or self._join
    collapse = kwargs.get('collapse', False)
    transform = kwargs.get('transform')
    # DEPRECATED handle the deprecated version of condense
    condense = kwargs.get('condensed',condense)

    # If collapse, also condense
    if collapse: condense = condense or True

    # Fugly range determination, all to get ourselves a start and end 
    # timestamp. Adjust steps argument to include the anchoring date.
    if end is None:
      if start is None:
        end = time.time()
        end_bucket = config['i_calc'].to_bucket( end )
        start_bucket = config['i_calc'].to_bucket( end, (-steps+1) )
      else:
        start_bucket = config['i_calc'].to_bucket( start )
        end_bucket = config['i_calc'].to_bucket( start, steps-1 )
    else:
      end_bucket = config['i_calc'].to_bucket( end )
      if start is None:
        start_bucket = config['i_calc'].to_bucket( end, (-steps+1) )
      else:
        start_bucket = config['i_calc'].to_bucket( start )
      
    # Now that we have start and end buckets, convert them back to normalized
    # time stamps and then back to buckets. :)
    start = config['i_calc'].from_bucket( start_bucket )
    end = config['i_calc'].from_bucket( end_bucket )
    if start > end: end = start
    
    interval_buckets = config['i_calc'].buckets(start, end)

    # If name is a list, then join all of results. It is more efficient to
    # use a single data structure and join "in-line" but that requires a major
    # refactor of the backends, so trying this solution to start with. At a
    # minimum we'd have to rebuild the results anyway because of the potential
    # for sparse data points would result in an out-of-order result.
    if isinstance(name, (list,tuple,set)):
      results = [ self._series(x, interval, config, interval_buckets, fetch=fetch, process_row=process_row) for x in name ]
      rval = self._join_results( results, config['coarse'], join_rows )
    else:
      rval = self._series(name, interval, config, interval_buckets, fetch=fetch, process_row=process_row)

    # If fine-grained, first do the condensed pass so that it's easier to do
    # the collapse afterwards. Be careful not to run the transform if there's
    # going to be another pass at condensing the data.
    if not config['coarse']:
      if condense:
        condense = condense if callable(condense) else self._condense
        for key in rval.iterkeys():
          data = condense( rval[key] )
          if transform and not collapse:
            data = self._process_transform(data, transform)
          rval[key] = data
      elif transform:
        for interval,resolutions in rval.items():
          for key in resolutions.iterkeys():
            resolutions[key] = self._process_transform(resolutions[key], transform)

    if config['coarse'] or collapse:
      if collapse:
        collapse = collapse if callable(collapse) else condense if callable(condense) else self._condense
        data = collapse(rval)
        if transform:
          rval = { rval.keys()[0] : self._process_transform(data, transform) }
        else:
          rval = { rval.keys()[0] : data }

      elif transform:
        for key,data in rval.items():
          rval[key] = self._process_transform(data, transform)
    
    return rval

  def _series(self, name, interval, config, buckets, **kws):
    '''
    Subclasses must implement fetching a series.
    '''
    raise NotImplementedError()

  def _join_results(self, results, coarse, join):
    '''
    Join a list of results. Supports both get and series.
    '''
    rval = OrderedDict()
    i_keys = set()
    for res in results:
      i_keys.update( res.keys() )
    for i_key in sorted(i_keys):
      if coarse:
        rval[i_key] = join( [res.get(i_key) for res in results] )
      else:
        rval[i_key] = OrderedDict()
        r_keys = set()
        for res in results:
          r_keys.update( res.get(i_key,{}).keys() )
        for r_key in sorted(r_keys):
          rval[i_key][r_key] = join( [res.get(i_key,{}).get(r_key) for res in results] )
    return rval

  def _process_transform(self, data, transform):
    '''
    Process transforms on the data.
    '''
    if isinstance(transform, (list,tuple,set)):
      return { t : self._transform(data,t) for t in transform }
    elif isinstance(transform, dict):
      return { tn : self._transform(data,tf) for tn,tf in transform.items() }
    return self._transform(data, transform)

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    raise NotImplementedError()

  def _insert(self, handle, key, value):
    '''
    Subclasses must implement inserting a value for a key.
    '''
    raise NotImplementedError()

  def _process_row(self, data):
    '''
    Subclasses should apply any read function to the data. Will only be called
    if there is one.
    '''
    raise NotImplementedError()

  def _condense(self, data):
    '''
    Condense a mapping of timestamps and associated data into a single 
    object/value which will be mapped back to a timestamp that covers all
    of the data.
    '''
    raise NotImplementedError()

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    raise NotImplementedError()


class Series(Timeseries):
  '''
  Simple time series where all data is stored in a list for each interval.
  '''

  def _type_no_value(self):
    return []

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if transform=='mean':
      total = sum( data )
      count = len( data )
      data = float(total)/float(count) if count>0 else 0
    elif transform=='count':
      data = len( data )
    elif transform=='min':
      data = min( data or [0])
    elif transform=='max':
      data = max( data or [0])
    elif transform=='sum':
      data = sum( data )
    elif callable(transform):
      data = transform(data)
    return data

  def _process_row(self, data):
    if self._read_func:
      return map(self._read_func, data)
    return data

  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    if data:
      return reduce(operator.add, data.values())
    return []

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    rval = []
    for row in rows:
      if row: rval.extend( row )
    return rval

class Histogram(Timeseries):
  '''
  Data for each interval is stored in a hash, counting occurrances of the
  same value within an interval. It is up to the user to determine the precision
  and distribution of the data points within the histogram.
  '''

  def _type_no_value(self):
    return {}

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if transform=='mean':
      total = sum( k*v for k,v in data.items() )
      count = sum( data.values() )
      data = float(total)/float(count) if count>0 else 0
    elif transform=='count':
      data = sum(data.values())
    elif transform=='min':
      data = min(data.keys() or [0])
    elif transform=='max':
      data = max(data.keys() or [0])
    elif transform=='sum':
      data = sum( k*v for k,v in data.items() )
    elif callable(transform):
      data = transform(data)
    return data

  def _process_row(self, data):
    rval = {}
    for value,count in data.items():
      if self._read_func: value = self._read_func(value)
      rval[ value ] = int(count)
    return rval
  
  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    rval = {}
    for resolution,histogram in data.items():
      for value,count in histogram.items():
        rval[ value ] = count + rval.get(value,0)
    return rval

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    rval = {}
    for row in rows:
      if row:
        for value,count in row.items():
          rval[ value ] = count + rval.get(value,0)
    return rval

class Count(Timeseries):
  '''
  Time series that simply increments within each interval.
  '''

  def _type_no_value(self):
    return 0

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if callable(transform):
      data = transform(data)
    return data
  
  def insert(self, name, value=1, timestamp=None, **kwargs):
    super(Count,self).insert(name, value, timestamp, **kwargs)

  def _process_row(self, data):
    return int(data) if data else 0

  def _condense(self, data):
    '''
    Condense by adding together all of the lists.
    '''
    if data:
      return sum(data.values())
    return 0

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    rval = 0
    for row in rows:
      if row: rval += row
    return rval

class Gauge(Timeseries):
  '''
  Time series that stores the last value.
  '''

  def _type_no_value(self):
    # TODO: resolve this disconnect with redis backend
    return 0

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if callable(transform):
      data = transform(data)
    return data

  def _process_row(self, data):
    if self._read_func:
      return self._read_func(data or '')
    return data

  def _condense(self, data):
    '''
    Condense by returning the last real value of the gauge.
    '''
    if data:
      data = filter(None,data.values())
      if data:
        return data[-1]
    return None

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    rval = None
    for row in rows:
      if row: rval = row
    return rval

class Set(Timeseries):
  '''
  Time series that manages sets.
  '''

  def _type_no_value(self):
    return set()

  def _transform(self, data, transform):
    '''
    Transform the data. If the transform is not supported by this series,
    returns the data unaltered.
    '''
    if transform=='mean':
      total = sum( data )
      count = len( data )
      data = float(total)/float(count) if count>0 else 0
    elif transform=='count':
      data = len(data)
    elif transform=='min':
      data = min(data or [0])
    elif transform=='max':
      data = max(data or [0])
    elif transform=='sum':
      data = sum(data)
    elif callable(transform):
      data = transform(data)
    return data

  def _process_row(self, data):
    if self._read_func:
      return set( (self._read_func(d) for d in data) )
    return data

  def _condense(self, data):
    '''
    Condense by or-ing all of the sets.
    '''
    if data:
      return reduce(operator.ior, data.values())
    return set()

  def _join(self, rows):
    '''
    Join multiple rows worth of data into a single result.
    '''
    rval = set()
    for row in rows:
      if row: rval |= row
    return rval

# Load the backends after all the timeseries had been defined.
try:
  from .redis_backend import RedisBackend
  BACKENDS['redis'] = RedisBackend
except ImportError as e:
  print('Redis backend not loaded,', e)

try:
  from .mongo_backend import MongoBackend
  BACKENDS['pymongo'] = MongoBackend
except ImportError as e:
  print('Mongo backend not loaded,', e)

try:
  from .sql_backend import SqlBackend
  BACKENDS['sqlalchemy'] = SqlBackend
except ImportError as e:
  print('SQL backend not loaded,', e)

try:
  from .cassandra_backend import CassandraBackend
  BACKENDS['cql'] = CassandraBackend
except ImportError as e:
  print('Cassandra backend not loaded,', e)
