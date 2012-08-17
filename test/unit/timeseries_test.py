'''
Tests for timeseries
'''

import time

from chai import Chai

from kairos.timeseries import *

class TimeseriesTest(Chai):

  def setUp(self):
    super(TimeseriesTest,self).setUp()
    self.series = Timeseries('client', type='series', prefix='foo', read_func='get', 
      write_func='set', intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60
        }
      })

  def test_new(self):
    t = Timeseries.__new__(Timeseries)
    assert_true( isinstance(t,Timeseries) )

    t = Timeseries.__new__(Timeseries, type='series')
    assert_true( isinstance(t, Series) )

    t = Timeseries.__new__(Timeseries, type='histogram')
    assert_true( isinstance(t, Histogram) )

    t = Timeseries.__new__(Timeseries, type='count')
    assert_true( isinstance(t, Count) )

  def test_init_with_no_args(self):
    t = Timeseries('client', type='series')
    assert_equals( 'client', t._client )
    assert_equals( None, t._read_func )
    assert_equals( None, t._write_func )
    assert_equals( '', t._prefix )
    assert_equals( {}, t._intervals )

  def test_init_with_args(self):
    assert_equals( 'client', self.series._client )
    assert_equals( 'get', self.series._read_func )
    assert_equals( 'set', self.series._write_func )
    assert_equals( 'foo:', self.series._prefix )

    assert_equals( 5*60, self.series._intervals['minute']['expire'] )
    assert_false( self.series._intervals['hour']['expire'] )
    assert_true( self.series._intervals['minute']['coarse'] )
    assert_false( self.series._intervals['hour']['coarse'] )

    now = time.time()
    i = r = int(now/60)
    assert_equals( (i, r, 'foo:test:minute:%s'%(i), 'foo:test:minute:%s:%s'%(i,r)), self.series._intervals['minute']['calc_keys']('test', now) )

    i = int(now/3600)
    r = int(now/60)
    assert_equals( (i, r, 'foo:test:hour:%s'%(i), 'foo:test:hour:%s:%s'%(i,r)), self.series._intervals['hour']['calc_keys']('test', now) )
