==========================================
Kairos - Time series data storage in Redis
==========================================

:Version: 0.0.7
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
  t = Timeseries(client, {
    'minute':{
      'step':60,            # 60 seconds
      'steps':120,          # last 2 hours
      'read_cast' : float,  # cast all results as floats
    }
  })

  t.insert('example', 3.14159)
  t.insert('example', 2.71828)
  print t.get('example', 'minute')

Each retrieval function will by default return an ordered dictionary, though
condensed results are also available. Run ``script/example`` to see standard
output; ``watch -n 4 script/example`` is a useful tool as well.


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


Future
======

* Tests
* Documentation
* Refactoring
* Storage of objects
* Histograms for compressing large intervals
* Interfaces to support easy integration with Python statistics packages
* Pipelining and other redis optimizations

License
=======

This software is licensed under the `New BSD License`. See the ``LICENSE.txt``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
