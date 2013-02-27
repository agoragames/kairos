==========================================
Kairos - Time series data storage in Redis
==========================================

:Version: 0.1.0
:Download: http://pypi.python.org/pypi/kairos
:Source: https://github.com/agoragames/kairos
:Keywords: python, redis, time, rrd, gevent

.. contents::
    :local:

.. _kairos-overview:

Overview
========

Kairos provides time series storage using a Redis backend. Kairos is intended 
to replace RRD in situations where the scale of Redis is required, with as
few dependencies on other packages as possible. It should work with 
`gevent <http://www.gevent.org/>`_ out of the box.

Requires python 2.7 or later.

Usage
=====

Install `redis <http://pypi.python.org/pypi/redis>`_ and kairos. ::

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

Each Timeseries will store data according to one of the supported types. The
keyword arguments to the constructor are: ::

  type
    One of (series, histogram, count). Optional, defaults to "series".

    series - each interval will append values to a list
    histogram - each interval will track count of unique values
    count - each interval will maintain a single counter

  prefix
    Optional, is a prefix for all keys in this histogram. If supplied
    and it doesn't end with ":", it will be automatically appended.

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

Each retrieval function will by default return an ordered dictionary, though
condensed results are also available. Run ``script/example`` to see standard
output; ``watch -n 4 script/example`` is a useful tool as well.

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

* Functional tests
* Interfaces to support easy integration with Python statistics packages
* Redis optimizations
* Mongo backend
* Ability to specify intervals in terms of days, weeks, etc.

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
