=================================
Kairos - Time series data storage
=================================

:Version: 0.9.0
:Download: http://pypi.python.org/pypi/kairos
:Source: https://github.com/agoragames/kairos
:Keywords: python, redis, mongo, sql, mysql, sqlite, postgresql, cassandra, timeseries, rrd, gevent, statistics

.. contents::
    :local:

.. _kairos-overview:

Overview
========

Kairos provides time series storage using Redis, Mongo, SQL or Cassandra 
backends. Kairos is intended to replace RRD and Whisper in situations where 
the scale and flexibility of other data stores is required. It works with
`gevent <http://www.gevent.org/>`_ and is the library on which
`torus <https://github.com/agoragames/torus>`_ is built.

Recommended for python 2.7 and later, it can work with previous versions if you
install `OrderedDict <https://pypi.python.org/pypi/ordereddict>`_.

Kairos provides a consistent API for a variety of timeseries types and the
storage engines they're implemented in. Each timestamp is resolved to a 
consistent bucket identifier ("interval") based on the number of whole seconds
since epoch, or a number corresponding to the Gregorian date associated with
the relative intervals  ``[daily, weekly, monthly, yearly]`` (e.g ``19990105``.
Within that, data can optionally be stored at resolutions (e.g. "daily, 
in 1 hour increments"). Multiple intervals can be tracked within a timeseries,
each with its own resolution and optional TTL.

In data stores that support it, TTLs can be set for automatically deleting 
data after a set number of intervals; other data stores expose an ``expire()``
method for deleting data programmatically.

Within each interval (or resolution), data is stored according to the type of
the timeseries and what each backend supports. The values tracked in each
timeseries can be loosely typed for backends that support it, else the type
will be whatever is set in the timeseries constructor. Even when loosely typed,
it should be assumed that the value should be a string or number.

series
  All data will be stored in the order in which it arrives. Uses data store
  list types where supported, else it will be timestamped records that 
  come as close as possible to the order in which they were written. Queries
  will return list objects.

histogram
  A hash of unique values to the number of its occurrences within an interval.
  Uses data store dictionaries where supported, else it will be separate 
  records for each unique value and timestamp. Queries will return dictionary
  objects.

count
  A simple counter will be maintained for each interval. Queries will return
  an integer.

gauge
  Stores the last-written value for each interval. Queries will return whatever
  the value type was.

set
  Stores all the unique values within an interval. Uses data store sets where
  supported, else it will be separate records for each unique value. Queries
  will return set objects.
    
Usage
=====

Kairos supports all storage engines using the same API. The constructor will 
return a Timeseries instance tailored for the type of data and the storage 
engine, and the API for updating and querying the timeseries is consistent 
for all combinations of data type and storage engine.

Constructor
-----------

The first argument is a handle to a supported storage engine or a URL (see 
below), and the rest of the keyword arguments configure the timeseries. The 
keyword arguments supported by all storage engines are:

type
  Optional, defaults to "series". 

read_func
  Optional, is a function applied to all values read back from the
  database. Without it, values will be strings for Redis, whatever 
  `write_func` defined for Mongo. Must accept a string value for Redis
  (empty string for no data) and can return anything.

write_func
  Optional, is a function applied to all values when writing. Can be
  used for histogram resolution, converting an object into an id, etc.
  Must accept whatever can be inserted into a timeseries and return an
  object which can be saved according to the rules of the storage engine.

intervals
  Required, a dictionary of interval configurations in the form of: ::

    {
      # interval name, used in keys and should conform to best 
      # practices according to the storage engine.
      minute: {
        
        # Required. The number of seconds that the interval will cover,
        # or a supported Gregorian interval.
        step: 60,
        
        # Optional. The maximum number of intervals to maintain. If supplied,
        # will use Redis and Mongo expiration to delete old intervals, else 
        # intervals exist in perpetuity. If the storage engine doesn't support
        # expiry, will be used to implement the expire() call.
        steps: 240,
        
        # Optional. Defines the resolution of the data, i.e. the number of 
        # seconds in which data is assumed to have occurred "at the same time".
        # So if you're tracking a month-long time series, you may only need 
        # resolution down to the day, or resolution=86400. Defaults to same
        # value as "step". Can also be a Gregorian interval.
        resolution: 60,
      }
    }

  In addition to specifying ``step`` and ``resolution`` in terms of seconds, 
  kairos also supports a simplified format for larger time intervals. For
  hours (h), days (d), weeks (w), months (m) and years (y), you can use 
  the format ``30d`` to represent 30 days, for example.

  As of ``0.3.0``, kairos also supports the Gregorian calendar for ``step``
  and ``resolution``. Either or both parameters can use the terms ``[daily,
  weekly, monthly, yearly]`` to describe an interval. You can also mix these
  terms between ``step`` and ``resolution`` (e.g. ``daily`` in 
  ``1h`` resolutions). The expiration time for Gregorian dates is still defined
  in terms of seconds and may not match the varying month lengths, leap years, 
  etc. Gregorian dates are translated into ``strptime``- and ``strftime``-compatible
  keys (as integers) and so may be easier to use in raw form or with any 
  external tools.

Storage Engines
---------------

Each of the supported storage engines also supports a set of keyword arguments
to configure their behavior. When intializing with a URL, the keyword argument
``client_config`` can optionally be a dictionary which will be passed as 
keyword arguments to the constructor for the client associated with the URL.
If kairos implements any custom keyword arguments from ``client_config`` those
are documented below.

Redis (redis://)
****************

An example timeseries stored in Redis: ::

  from kairos import Timeseries
  import redis

  client = redis.Redis('localhost', 6379)
  t = Timeseries(client, type='histogram', read_func=int, intervals={
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Additional keyword arguments are: ::

  prefix
    Optional, is a prefix for all keys in this timeseries. If 
    supplied and it doesn't end with ":", it will be automatically appended.

Supported URL `formats <https://github.com/andymccurdy/redis-py/blob/master/redis/client.py#L332>`_: ::

  redis://localhost
  redis://localhost/3

All `supported <https://github.com/andymccurdy/redis-py/blob/master/redis/client.py#L361>`_ configuration options can be passed in ``client_config``.

Mongo (mongodb://)
******************

An example timeseries stored in Mongo: ::

  from kairos import Timeseries
  import pymongo

  client = pymongo.MongoClient('localhost')
  t = Timeseries(client, type='histogram', read_func=float, intervals={
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Additional keyword arguments are: ::

  escape_character
    Optional, defines the character used to escape periods. Defaults to the
    unicode character "U+FFFF". 

Supported URL `formats <http://docs.mongodb.org/manual/reference/connection-string/>`_: ::

  mongodb://localhost
  mongodb://localhost:27018/timeseries
  mongodb://guest:host@localhost/authed_db


All `supported <http://api.mongodb.org/python/current/api/pymongo/mongo_client.html>`_ configuration arguments can be passed in ``client_config``, in addition to: ::

  database
    The name of the database to use. Defaults to 'kairos'. Required if using
    an auth database. Overrides any database provided in the URL.

SQL (*sql*://)
**************

An example timeseries stored in a SQLite memory store: ::

  from kairos import Timeseries
  from sqlalchemy import create_engine

  client = create_engine('sqlite:///:memory:')
  t = Timeseries(client, type='histogram', read_func=int, intervals={
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Additional keyword arguments are: ::

  string_length
    Optional, configures the length of strings (VARCHARs). Defaults to 255.
    All tables have at least 2 string columns, and the size of these columns
    may impact usability of the SQL storage engine.

  text_length
    Optional, configures the length of TEXT and BLOB columns. Defaults to 
    32Kbytes. Only matters if value_type is a text or blob.

  table_name
    Optional, overrides the default table name for a timeseries type.

  value_type
    Optional, defines the type of value to be stored in the timeseries. 
    Defaults to float. Can be a string, a Python type or a SQLAlchemy type
    or instance.
    
    'blob'
    'bool'
    <type 'bool'>
    'boolean'
    'clob'
    'date'
    <type 'datetime.date'>
    'datetime'
    <type 'datetime.datetime'>
    'decimal'
    <class 'decimal.Decimal'>
    'float'
    <type 'float'>
    'int'
    'int64'
    'integer'
    <type 'int'>
    'long'
    <type 'long'>
    'str'
    'string'
    <type 'str'>
    'text'
    'time'
    <type 'datetime.time'>
    'unicode'
    <type 'unicode'>

Supported URL `formats <http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#database-urls>`_ are many and varied. A few examples: ::

  sqlite:///:memory:
  postgresql://scott:tiger@localhost/mydatabase
  mysql+mysqldb://scott:tiger@localhost/foo
  oracle://scott:tiger@127.0.0.1:1521/sidname

All `supported <http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#sqlalchemy.create_engine>`_ constructor arguments can be used in ``client_config``.

Cassandra (cassandra://, cql://)
********************************

An example timeseries stored in Cassandra: ::

  from kairos import Timeseries
  import cql

  client = cql.connect('localhost', 9160, 'keyspace', cql_version='3.0.0')
  t = Timeseries(client, type='histogram', read_func=int, intervals={
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Additional keyword arguments are: ::

  table_name
    Optional, overrides the default table name for a timeseries type.

  pool_size
    Optional, set a cap on the pool size. Defines the maximum number of
    connections to maintain in the pool. Defaults to 0 for no maximum.

  value_type
    Optional, defines the type of value to be stored in the timeseries. 
    Defaults to float. Can be a string or a Python type.

    <type 'unicode'>
    string
    decimal
    <type 'long'>
    int
    double
    unicode
    float
    long
    <type 'bool'>
    <type 'float'>
    boolean
    int64
    str
    text
    blob
    clob
    integer
    bool
    <type 'str'>
    <type 'int'>
    inet

Supported URL formats are: ::
  
  cql://
  cassandra://localhost:9160
  cassandra://localhost/database

There are no special arguments supported in ``client_config``.

kairos requires `cql <https://pypi.python.org/pypi/cql>`_ as it supports
`CQL3 <https://cassandra.apache.org/doc/cql3/CQL.html>`_ and gevent. This 
requires that the keyspace be created before the connection, and the keyword 
argument ``cql_version='3.0.0'`` must be used.

A notable downside of this library is that it does not support a list of
endpoints to connect to, so is missing key High Availability features.

It is likely that future versions of kairos will require 
`cassandra-driver <https://github.com/datastax/python-driver>`_ when it 
is ready.

Cassandra counters can only store integers, and cannot be used for a 
running total of floating point numbers.

Kairos implements a connection pooling mechanism on top of `cql`. The pool
is a simple soft-cap on the number of connections maintained in the pool,
but not necessarily the total number of connections at a time. An optional
hard cap may be implemented in a future release.

Inserting Data
--------------

There are two methods to insert data, ``Timeseries.insert`` and ``Timeseries.bulk_insert``.

insert
******

* **name** The name of the statistic
* **value** The value of the statistic (optional for count timeseries), or a list of values
* **timestamp** `(optional)` The timestamp of the statistic, defaults to ``time.time()`` if not supplied
* **intervals** `(optional)` The number of time intervals before (<0) or after (>0) ``timestamp`` to copy the data
* **\*\*kwargs** `(optional)` Any additional keyword arguments supported by a backend, see below

For ``series`` and ``histogram`` timeseries types, ``value`` can be whatever 
you'd like, optionally processed through the ``write_func`` method before being 
written to storage. Depending on your needs, ``value`` (or the output of 
``write_func``) does not have to be a number, and can be used to track such 
things as unique occurances of a string or references to other objects, such 
as MongoDB ObjectIds. Note that many of the aggregate functions in ``histogram``
expect the data to be real numbers.

For the ``count`` type, ``value`` is optional and should be a float or integer 
representing the amount by which to increment or decrement ``name``; it defaults
to ``1``.

For the ``gauge`` type, ``value`` can be anything and it will be stored as-is.

For all timeseries types, if ``value`` is one of ``(list,tuple,set)``, will 
call ``bulk_insert``.

The ``intervals`` option allows the caller to simulate the value appearing in
time periods before or after the ``timestamp``. This is useful for creating 
fast trending (e.g. "count over last seven days"). It is important to note 
that, because the time periods are simulated, resolution is lost for the
the simulated timestamps.

Redis
#####

Redis supports an additional keyword argument, ``pipeline``, to give the caller
control over batches of commands. If ``pipeline`` is supplied, the ``execute``
method will not be called and it is up to the caller to do so.

bulk_insert
***********

* **inserts** The structure of inserts (see below)
* **intervals** `(optional)` The number of time intervals before (<0) or after (>0) ``timestamp`` to copy the data
* **\*\*kwargs** `(optional)` Any additional keyword arguments supported by a backend, see below

The ``inserts`` field must take the following form: ::

    {
      timestamp : {
        name: [ value, ... ],
        ...
      },
      ...
    }

The meaning of ``timestamp``, ``name`` and ``value`` are identical to those 
parameters in ``insert``. The caller can insert any number of timestamps,
statistic names and values, and the backend will optimize the insert where
possible. See details on the different backends below. Where a backend does
not support an optimized bulk insert, the data structure will be processed
such that each value will be passed to ``insert``.

The ``inserts`` structure can be a ``dict`` or ``OrderedDict``. If you need
the insert order preserved, such as when inserting into a ``series`` or 
``gauge``, you should use ``OrderedDict``.

If ``timestamp`` is unknown, use ``None`` for the key and it will be set to
the current value of ``time.time()``. Note that this may alter ordering if
``inserts`` is an ``OrderedDict``.

**NOTE** bulk inserts will increase memory usage of the client process.

Redis
#####

Redis bulk inserts are implemented by using a single pipeline (without
transactions) and committing the pipeline after all bulk inserts have been
executed. The bulk insert also supports the ``pipeline`` argument, with the
same rules as ``insert``.

Mongo
#####

Mongo bulk inserts are implemented by joining all of the data together into
a condensed set of queries and updates. As the configuration of a timeseries
may result in multiple timestamps resolving to the same record (e.g. per-day
data), this could result in significant performance gains when the timeseries
is a ``count``, ``histogram`` or ``gauge``.

SQL
###

There is no optimization for bulk inserts in SQL due to the lack of 
native update-or-insert support. The generic SQL implementation requires an
attempted update to be committed before kairos can determine if an insert is
required. Future versions may have optimized implementations for specific
SQL servers which support such a feature, at which time bulk inserts may be
optimized for those specific backends.

Cassandra
#########

The ``cql`` library has no support for transactions, grouping, etc.

Meta Data
---------

There are two methods to query meta data about a Timeseries.

list
****

There are no arguments. Returns a list of all of the stat names stored 
in the Timeseries.

properties
**********

Takes a single argument, the name of the timeseries. Returns a dictionary
with the following fields: ::

  { interval : { 'first' : timestamp, 'last' : timestamp } }

``interval`` will be the named interval, such as "minute". For each interval,
there is a dictionary of properties. ``first`` is the timestamp of the first
data point in the timeseries, and ``last`` is the last data point in the 
timeseries.


Reading Data
------------

There are three methods to read data, ``Timeseries.get``, ``Timeseries.series``
and ``Timeseries.iterate``. ``get`` will return data from a single bucket, 
and ``series`` will return data from several buckets. ``iterate`` will use
the ``Timeseries.properties`` method to determine the date range of the data,
and return a generator that calls ``get`` for every possible interval in
the date range.

get
***

Supports the following parameters. All optional parameters are keyword arguments.

* **name** The name of the statistic, or a list of names whose data will be joined together.
* **interval** The named interval to read from
* **timestamp** `(optional)` The timestamp to read, defaults to ``time.time()``
* **condensed** `(optional)` **DEPRECATED** Use ``condense`` instead. Support for this will be removed entirely in a future release.
* **transform** `(optional)` Optionally process each row of data. Supports ``[mean, count, min, max, sum]``, or any callable that accepts datapoints according to the type of series (e.g histograms are dictionaries, counts are integers, etc). Transforms are called after ``read_func`` has cast the data type and after resolution data is optionally condensed. If ``transform`` is one of ``(list,tuple,set)``, will load the data once and run all the transforms on that data set. If ``transform`` is a ``dict`` of the form ``{ transform_name : transform_func }``, will run all of the transform functions on the data set.
* **fetch** `(optional)` Function to use instead of the built-in implementations for fetching data. See `Customized Reads`_.
* **process_row** `(optional)` Can be a callable to implement `Customized Reads`_.
* **condense** `(optional)` If using resolutions, ``True`` will collapse the resolution data into a single row. Can be a callable to implement `Customized Reads`_.
* **join_rows** `(optional)` Can be a callable to implement `Customized Reads`_.

Returns a dictionary of ``{ timestamp : data }``, where ``timestamp`` is a Unix timestamp
and ``data`` is a data structure corresponding to the type of series, or whatever 
``transform`` returns.  If not using resolutions or ``condense=True``, the length 
of the dictionary is 1, else it will be the number of resolution buckets within
the interval that contained data. If ``transform`` is a list, ``data`` will be a 
dictionary of ``{ transform_func : transformed_data }``. If ``transform`` is a ``dict``,
``data`` will be a dictionary of ``{ transform_name : transformed_data }``.

series
******

Almost identical to ``get``, supports the following parameters. All optional parameters are keyword arguments.

* **name** The name of the statistic, or a list of names whose data will be joined together.
* **interval** The named interval to read from
* **start** `(optional)` The timestamp which should be in the first interval of the returned data.
* **end** `(optional)` The timestamp which should be in the last interval of the returned data. 
* **steps** `(optional)` The number of steps in the interval to read, defaults to either ``steps`` in the configuration or 1. Ignored if both ``start`` and ``end`` are defined. If either ``start`` or ``end`` are defined, ``steps`` is inclusive of whatever interval that timestamp falls into.
* **condensed** `(optional)` **DEPRECATED** Use ``condense`` instead. Support for this will be removed entirely in a future release.
* **transform** `(optional)` Optionally process each row of data. Supports ``[mean, count, min, max, sum]``, or any callable that accepts a list of datapoints according to the type of series (e.g histograms are dictionaries, counts are integers, etc). Transforms are called after ``read_func`` has cast the data type and after resolution data is optionally condensed. If ``transform`` is one of ``(list,tuple,set)``, will load the data once and run all the transforms on that data set. If ``transform`` is a ``dict`` of the form ``{ transform_name : transform_func }``, will run all of the transform functions on the data set.
* **fetch** `(optional)` Function to use instead of the built-in implementations for fetching data. See `Customized Reads`_.
* **process_row** `(optional)` Can be a callable to implement `Customized Reads`_.
* **condense** `(optional)` If using resolutions, ``True`` will collapse the resolution data into a single row. Can be a callable to implement `Customized Reads`_.
* **join_rows** `(optional)` Can be a callable to implement `Customized Reads`_.
* **collapse** `(optional)` ``True`` will collapse all of the data in the date range into a single result. Can be a callable to implement `Customized Reads`_.

Returns an ordered dictionary of ``{ interval_timestamp : { resolution_timestamp: data } }``,
where ``interval_timestamp`` and ``resolution_timestamp`` are Unix timestamps
and ``data`` is a data structure corresponding to the type of series, or whatever 
``transform`` returns.  If not using resolutions or ``condense=True``, the dictionary
will be of the form ``{ interval_timestamp : data }``.

All variations of ``transform`` and the resulting format of ``data`` are the same
as in ``get``.

If both ``start`` and ``end`` are defined, the returned data will start and end
on intervals including those timestamps. If only ``start`` is defined, then the
return data will start with an interval that includes that timestamp, with the
total number of intervals returned defined by ``steps``. If only ``end`` is 
defined, then the return data will end with an interval that includes that 
timestamp, with the total number of intervals preceeding it defined by ``steps``.

It is important to note that the interval timestamps in the returned data will
not necessarily match ``start`` or ``end``. This is because of the consistent
hashing scheme that kairos uses, such that ``start`` and ``end`` will be 
translated into the bucket in which it can be found.

iterate
*******

Almost identical to ``get`` except it does not accept a ``timestamp`` argument.

* **name** The name of the statistic, or a list of names whose data will be joined together.
* **interval** The named interval to read from
* **transform** `(optional)` Optionally process each row of data. Supports ``[mean, count, min, max, sum]``, or any callable that accepts datapoints according to the type of series (e.g histograms are dictionaries, counts are integers, etc). Transforms are called after ``read_func`` has cast the data type and after resolution data is optionally condensed. If ``transform`` is one of ``(list,tuple,set)``, will load the data once and run all the transforms on that data set. If ``transform`` is a ``dict`` of the form ``{ transform_name : transform_func }``, will run all of the transform functions on the data set.
* **fetch** `(optional)` Function to use instead of the built-in implementations for fetching data. See `Customized Reads`_.
* **process_row** `(optional)` Can be a callable to implement `Customized Reads`_.
* **condense** `(optional)` If using resolutions, ``True`` will collapse the resolution data into a single row. Can be a callable to implement `Customized Reads`_.
* **join_rows** `(optional)` Can be a callable to implement `Customized Reads`_.

Returns a generator which iterates over ``( timestamp, data )`` tuples, where
``timestamp`` is a Unix timestamp and ``data`` corresponds to the rules
documented in ``get``. Yields a tuple for each potential timestamp in the
entire date range of the timeseries, even if there is no data. 


Customized Reads
----------------

**ALPHA** This feature is still being explored and the API may change significantly.

There are times when the data in a timeseries requires processing to
be pushed onto the datastore. 

There are times when one needs custom control over the reading and processing
of data in a timeseries. As there is no good way to do this generically,
the ``get`` and ``series`` API supports several keyword arguments to customize
access to the data. Common use cases are to handle large sets of data that
can be processed in the datastore, and situations where one wants to implement
cutom analysis of the dataset such as calculating variance. 

General
*******

The following functions can be overloaded with keyword parameters to ``get`` and
``series`` (``collapse`` being only used for a series).

fetch
#####

A customized database read function. The usage varies depending on the backends
which are described in detail below.
**IMPORTANT** You are welcome to change the type of the return value, but be
wary that transforms, condense and collapse functionality may not work
properly with the changed data types.

process_row
###########

The function which handles the type casting of the data read from the backend
and also calling the ``read_func`` if it has been defined for the time series.
It is required that you define this function if you overload ``fetch`` such
that the returned data type is not the same as the time series' native format
(``dict`` for histogram, ``list`` for series, etc).

The function must be in the form of ``process_row(data)``, where:

* **data** The row data generated by the native or ``fetch`` implementation, not
  including any time stamps.

The function may return any data type, but if it's not the native format of the
time series, additional downstream functions may have to be overloaded.

condense
########

If the ``condense`` argument is a callable, the caller can override how resolution
data is collapsed (reduced) into a single interval. The argument will always be 
in the form of: ::

  {
    'resolution_t0' : <data_t0>,
    'resolution_t1' : <data_t1>,
    ...
    'resolution_tN' : <data_tN>,
  }

Where ``<data_tN>`` is the data returned from the native or ``fetch`` 
implementation and passed through the native or custom ``process_row``
implementation.

The function should return a single value, optionally in the same format as 
``<data_tN>``, but this method could also be used for calculating such
things as rate of change or variance within a time interval.

join_rows
#########

If the ``join_rows`` argument is a callable and the ``name`` parameter to ``get``
or ``series`` is one of ``(list,tuple,set)``, this method will be called to join
the data from several named timeseries into a single result. The argument will
always be in the form of: ::

  [
    <data_series0>,
    <data_series1>,
    ...
    <data_seriesN>
  ]

Where ``<data_series0>`` will be the data within a single timestamp window in
the series' native format or whatever was generated by custom implementations
of ``fetch``, ``process_row`` and/or ``condense``. It is important to note
that not every series will contain data points within a given time interval.

In addition to reducing multiple time series' worth of data within an interval
into a single result, this method could be used to implement cross-series
analytics such as unions, intersections and differentials.

collapse
########

If the ``collapse`` argument is a callable, the caller can override how interval
data is collapsed (reduced) into a single result. The native implementation is to
call the ``condense`` function implemented by a time series. The arguments are
the same as a custom ``condense`` function, as-is the expected return value.

It's important to note that if ``collapse`` is defined, the series will 
automatically be condensed as well, so if ``fetch`` is overloaded to return a 
custom data type, then ``condense`` must also be defined. If ``collapse`` is
``True``, the custom ``condense`` function will be used if defined.

In addition to collapsing the result of a time series into a single data set,
this method could also be used to calculate data across a time series, such as
variance.

transform
#########

As noted previously, ``transform`` can be any callable, list of names or callables,
or a named map of transform names or callables. The transforms will be processed 
after all previous native or custom read functions, including ``collapse``.

Redis
*****

The function must be in the form of ``fetch(handle, key)``, where:

* **handle** Either a Redis client or pipeline instance
* **key** The key for the timeseries data

The return value should correspond to the data type of timeseries, e.g. ``dict``
for a histogram. One should always assume that ``handle`` is both a pipeline
`and` a client, and ``fetch`` should return the result of, e.g. 
``handle.hlen(...)``, but that it cannot be used to return a literal, such
as ``lambda: h,k: { 'foo' : h.hlen(k) }``

Mongo
*****

The function must be in the form of ``fetch(handle, **kwargs)``, where:

* **handle** A PyMongo ``Collection``
* **spec** The (suggested) query specification
* **sort** The (suggested) sort definition for the query
* **method** The suggested method to use on the ``handle``

The required return value depends on the value of ``method``.

* **find_one** Should return a hash in the form ``{ value : <data> }``, where
  ``<data>`` should correspond to the data type of the timeseries, e.g. ``list``
  for a series. May directly return a result from ``pymongo.collection.find_one``.
* **find** Should return an iterable in the form ``[ { value: <data> }, ... ]``,
  where ``<data>`` follows the same rules as ``find_one``.

Re-implementing the default functionality would look like: ::

  def mongo_fetch(handle, spec, sort, method):
    if method=='find':
      return handle.find( spec=spec, sort=sort )
    elif method=='find_one':
      return handle.find_one( spec )

SQL
***

The function must be in the form 
``fetch(connection, table, name, interval, i_start, i_end)``, where:

* **connection** A SQLAlchemy ``Connection``
* **table** A SQLAlchemy ``Table``
* **name** The name of the stat to fetch
* **interval** The interval of the stat to fetch
* **i_start** The interval timestamp (starting) key
* **i_end** (optional) For a series, the ending timestamp key

The return value should be in the form of ::

  { 
    'interval_t0' : {
      'resolution_t0t0' : <data_t0t0>,
      'resolution_t0t1' : <data_t0t1>,
      ...
      'resolution_t0tN' : <data_t0tN>
    },
    'interval_t1' : { ... },
    ...
    'interval_tN' : { ... },
  }

If the series doesn't use resolutions, then ``resolution_tNtN`` should be 
``None``, and so each interval will be in the form 
``{ 'interval_tN: { None : <data_tN> } }``. This is inherent in the way that
data is stored within the tables.

If ``i_end`` is supplied, the query should be over the range 
``i_time >= i_start AND i_time <= i_end``, else the query should be for
the interval ``i_time = i_start``.

Cassandra
*********

The function must be in the form 
``fetch(connection, table, name, interval, i_start, i_end)``, where:

* **cursor** A ``cql`` ``Connection``
* **table** The name of the table
* **name** The name of the stat to fetch
* **interval** The interval of the stat to fetch
* **intervals** The list of interval timestamps

The return value should be in the form of ::

  { 
    'interval_t0' : {
      'resolution_t0t0' : <data_t0t0>,
      'resolution_t0t1' : <data_t0t1>,
      ...
      'resolution_t0tN' : <data_t0tN>
    },
    'interval_t1' : { ... },
    ...
    'interval_tN' : { ... },
  }

If the series doesn't use resolutions, then ``resolution_tNtN`` should be 
``None``, and so each interval will be in the form 
``{ 'interval_tN: { None : <data_tN> } }`` and can be determined when a row
has an ``r_time`` of ``-1``.

If ``intervals`` is a list of 1, it's effectively a ``get`` query where
``i_time = intervals[0]``, else it's ``i_time >= intervals[0] AND
i_time <= intervals[-1]``. The full list of intervals is supplied to workaround
Cassandra's lack of grouping support in situations where an aggregate per
``i_time`` is desired.


Deleting Data
-------------

delete
******

Takes a single argument, the name of the timeseries. Will delete all data for 
that timeseries in all intervals.

delete_all
**********

Deletes every timeseries for all intervals. This method may be fast in data
stores that support optimized deletes, else it will have to delete for each
timeseries returned in ``list``.

expire
******

Takes a single argument, the name of the timeseries. For storage engines that 
do not support expiry, such as SQL, will delete expired data from intervals
for which ``steps`` is defined. All other storage engines will raise the
``NotImplementedError`` exception.

Dragons!
--------

Kairos achieves its efficiency by using TTLs and data structures
in combination with a key naming scheme that generates consistent keys based on
any timestamp relative to epoch. However, just like 
`RRDtool <http://oss.oetiker.ch/rrdtool/>`_, changing any attribute of the
timeseries means that new data will be stored differently than old data. For
this reason it's best to completely delete all data in an old time series
before creating or querying using a new configuration.

If you want to migrate data, there are tools in 
`torus <https://github.com/agoragames/torus>`_ that can help.


Installation
============

Kairos is available on `pypi <http://pypi.python.org/pypi/kairos>`_ and can be
installed using ``pip`` ::

  pip install kairos


If installing from source:

* with development requirements (e.g. testing frameworks) ::

    pip install -r development.pip

* without development requirements ::

    pip install -r requirements.pip

Note that kairos does not install packages for any of the supported backends,
and that you must do this yourself.

Tests
=====

Use `nose <https://github.com/nose-devs/nose/>`_ to run the test suite. ::

  $ nosetests

The test suite can be controlled through several environment variables, all
defaulting to ``true``. 

* **TEST_REDIS** *true*
* **TEST_MONGO** *true*
* **TEST_SQL** *true*
* **TEST_CASSANDRA** *true*
* **TEST_SERIES** *true*
* **TEST_HISTOGRAM** *true*
* **TEST_COUNT** *true*
* **TEST_GAUGE** *true*
* **TEST_SET** *true*
* **SQL_HOST** *sqlite:///:memory:*
* **CASSANDRA_KEYSPACE** *kairos*

Roadmap
=======

* Round-robbin intervals for datastores without TTLs
* Round-robbin databases: memcache (and compatible, e.g. ElastiCache), Riak,
  DynamoDB, SimpleDB, GDBM, Berkeley DB, and more
* Redis optimizations
* Capped collection support for mongo
* Python 3 support
* InfluxDB support
* Bloom filters
* Joined series populate a data structure at query time
* Joined series support concurrency "runner"

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
