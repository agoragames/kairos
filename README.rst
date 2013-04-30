====================================================
Kairos - Time series data storage in Redis and Mongo
====================================================

:Version: 0.2.1
:Download: http://pypi.python.org/pypi/kairos
:Source: https://github.com/agoragames/kairos
:Keywords: python, redis, mongo, time, timeseries, rrd, gevent, statistics

.. contents::
    :local:

.. _kairos-overview:

Overview
========

Kairos provides time series storage using Redis or Mongo backends. Kairos is 
intended to replace RRD and Whisper in situations where the scale and 
flexibility of Redis or Mongo is required. It works with
`gevent <http://www.gevent.org/>`_ out of the box.

Recommended for python 2.7 and later, it can work with previous versions if you
install `OrderedDict <https://pypi.python.org/pypi/ordereddict>`_.

Usage
=====

Kairos supports redis and mongo storage using the same API.

Redis
-----

::

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

Mongo
-----

::

  from kairos import Timeseries
  import pymongo

  client = pymongo.MongoClient('localhost')
  t = Timeseries(client, type='histogram', read_func=int, intervals={
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Each Timeseries will store data according to one of the supported types. The
keyword arguments to the constructor are: ::

  type
    One of (series, histogram, count). Optional, defaults to "series".

    series - each interval will append values to a list
    histogram - each interval will track count of unique values
    count - each interval will maintain a single counter
    gauge - each interval will store the most recent data point

  prefix
    Optional, redis only, is a prefix for all keys in this timeseries. If 
    supplied and it doesn't end with ":", it will be automatically appended.

  read_func
    Optional, is a function applied to all values read back from the
    database. Without it, values will be strings. Must accept a string
    value (empty string for no data) and can return anything.

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

In addition to specifying ``step`` and ``resolution`` in terms of seconds, 
kairos also supports a simplified format for larger time intervals. For
hours (h), days (d), weeks (w), months (m) and years (y), you can use 
the format ``30d`` to represent 30 days, for example.

Each retrieval function will by default return an ordered dictionary, though
condensed results are also available. Run ``script/example`` to see standard
output; ``watch -n 4 script/example`` is a useful tool as well.

Inserting Data
--------------

There is one method to insert data, ``Timeseries.insert`` which takes the
followng arguments:


* **name** The name of the statistic
* **value** The value of the statistic (optional for count timeseries)
* **timestamp** `(optional)` The timestamp of the statstic, defaults to ``time.time()`` if not supplied

For ``series`` and ``histogram`` timeseries types, ``value`` can be whatever 
you'd like, optionally processed through the ``write_func`` method before being 
written to storage. Depending on your needs, ``value`` (or the output of 
``write_func``) does not have to be a number, and can be used to track such 
things as unique occurances of a string or references to other objects, such 
as MongoDB ObjectIds.

For the ``count`` type, ``value`` is optional and should be a float or integer 
representing the amount by which to increment or decrement ``name``; it defaults
to ``1``.

For the ``gauge`` type, ``value`` can be anything and it will be stored as-is.

Data for all timeseries is stored in "buckets", where any Unix timestamp will
resolve a consistent bucket name according to the ``step`` and ``resolution``
attributes of a schema. A bucket will contain the following data structures for
the corresponding series type.

* **series** list
* **histogram** dictionary (map)
* **count** integer or float


Reading Data
------------

There are two methods to read data, ``Timeseries.get`` and ``Timeseries.series``.
``get`` will return data from a single bucket, and ``series`` will return data
from several buckets.

get
***

Supports the following parameters:

* **name** The name of the statistic
* **interval** The named interval to read from
* **timestamp** `(optional)` The timestamp to read, defaults to ``time.time()``
* **condensed** `(optional)` If using resolutions, ``True`` will collapse the resolution data into a single row
* **transform** `(optional)` Optionally process each row of data. Supports ``[mean, count, min, max, sum]``, or any callable that accepts datapoints according to the type of series (e.g histograms are dictionaries, counts are integers, etc). Transforms are called after ``read_func`` has cast the data type and after resolution data is optionally condensed.

Returns a dictionary of ``{ timestamp : data }``, where ``timestamp`` is a Unix timestamp
and ``data`` is a data structure corresponding to the type of series, or whatever 
``transform`` returns.  If not using resolutions or ``condensed=True``, the length 
of the dictionary is 1, else it will be the number of resolution buckets within
the interval that contained data.

series
******

Almost identical to ``get``, supports the following parameters:

* **name** The name of the statistic
* **interval** The named interval to read from
* **steps** `(optional)` The number of steps in the interval to read, defaults to either ``steps`` in the configuration or 1.
* **timestamp** `(optional)` The timestamp of the last step to read, defaults to ``time.time()``; i.e. ``steps`` is the number of steps before ``timestamp``.
* **condensed** `(optional)` If using resolutions, ``True`` will collapse the resolution data into a single row
* **transform** `(optional)` Optionally process each row of data. Supports ``[mean, count, min, max, sum]``, or any callable that accepts a list of datapoints according to the type of series (e.g histograms are dictionaries, counts are integers, etc). Transforms are called after ``read_func`` has cast the data type and after resolution data is optionally condensed.

Returns a dictionary of ``{ timestamp : { resolution_timestamp: data } }``, where 
``timestamp`` and ``resolution_timestamp`` are Unix timestamps
and ``data`` is a data structure corresponding to the type of series, or whatever 
``transform`` returns.  If not using resolutions or ``condensed=True``, the dictionary
will be of the form ``{ timestamp : data }``.


Dragons!
--------

Kairos achieves its efficiency by using Redis' TTLs and data structures in 
combination with a key naming scheme that generates consistent keys based on
any timestamp relative to epoch. However, just like 
`RRDtool <http://oss.oetiker.ch/rrdtool/>`_, changing any attribute of the
timeseries means that new data will be stored differently than old data. For
this reason it's best to completely delete all data in an old time series
before creating or querying using a new configuration.


Installation
============

Kairos is available on `pypi <http://pypi.python.org/pypi/kairos>`_ and can be installed using ``pip`` ::

  pip install kairos


If installing from source:

* with development requirements (e.g. testing frameworks) ::

    pip install -r development.pip

* without development requirements ::

    pip install -r requirements.pip

Note that kairos does not by default require the redis package, nor does
it require `hiredis <http://pypi.python.org/pypi/hiredis>`_ though it is
strongly recommended.

Tests
=====

Use `nose <https://github.com/nose-devs/nose/>`_ to run the test suite. ::

  $ nosetests


Future
======

* Redis optimizations
* Bloom filters
* "Native" transforms that leverage data store features (e.g. "length")

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
