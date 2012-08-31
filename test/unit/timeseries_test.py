'''
Tests for timeseries
'''

import time

from chai import Chai

from kairos.timeseries import *

# HACK for ease of testing
class SortedDict(dict):
  def iteritems(self):
    for key in sorted(self.keys()):
      yield (key, self[key])

class TimeseriesTest(Chai):

  def setUp(self):
    super(TimeseriesTest,self).setUp()

    self.client = mock()
    self.series = Timeseries(self.client, type='series', prefix='foo', read_func=mock(), 
      write_func=mock(), intervals=SortedDict({
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      }) )
    self.series._intervals['minute']['calc_keys'] = mock()
    self.series._intervals['hour']['calc_keys'] = mock()

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
    t = Timeseries(self.client, type='series', prefix='foo', read_func='get', 
      write_func='set', intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    assert_equals( self.client, t._client )
    assert_equals( 'get', t._read_func )
    assert_equals( 'set', t._write_func )
    assert_equals( 'foo:', t._prefix )

    assert_equals( 5*60, t._intervals['minute']['expire'] )
    assert_false( t._intervals['hour']['expire'] )
    assert_true( t._intervals['minute']['coarse'] )
    assert_false( t._intervals['hour']['coarse'] )

    now = time.time()
    i = r = int(now/60)
    assert_equals( (i, r, 'foo:test:minute:%s'%(i), 'foo:test:minute:%s:%s'%(i,r)), 
      t._intervals['minute']['calc_keys']('test', now) )

    i = int(now/3600)
    r = int(now/60)
    assert_equals( (i, r, 'foo:test:hour:%s'%(i), 'foo:test:hour:%s:%s'%(i,r)), 
      t._intervals['hour']['calc_keys']('test', now) )

  def test_insert_with_timestamp_and_write_func(self):
    pipeline = mock()
    self.series._write_func = mock()
    
    expect( self.series._write_func ).args('val').returns('value')
    expect( self.client.pipeline ).returns( pipeline )

    # not coarse hour
    expect( self.series._intervals['hour']['calc_keys'] ).args('name',3.14).returns( ('hibucket', 'hrbucket', 'hikey','hrkey') )
    expect( pipeline.sadd ).args( 'hikey', 'hrbucket' )
    expect( self.series._insert ).args( pipeline, 'hrkey', 'value' )

    # coarse minute
    expect( self.series._intervals['minute']['calc_keys'] ).args('name',3.14).returns( ('mibucket', 'mrbucket', 'mikey','mrkey') )
    expect( self.series._insert ).args( pipeline, 'mikey', 'value' )
    expect( pipeline.expire ).args( 'mikey', 5*60 )

    expect( pipeline.execute )

    self.series.insert('name', 'val', timestamp=3.14)

  def test_insert_with_no_timestamp_and_fine_expiry(self): 
    pipeline = mock()
    self.series._write_func = mock()
    
    expect( self.series._write_func ).args('val').returns('value')
    expect( self.client.pipeline ).returns( pipeline )
    
    # set resolution and expiry
    self.series._intervals['minute']['resolution'] = 2

    # not coarse hour
    expect( self.series._intervals['hour']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( ('hibucket', 'hrbucket', 'hikey','hrkey') )
    expect( pipeline.sadd ).args( 'hikey', 'hrbucket' )
    expect( self.series._insert ).args( pipeline, 'hrkey', 'value' )
    expect( pipeline.expire ).args( 'hikey', 5*60 )
    expect( pipeline.expire ).args( 'hrkey', 5*60 )

    # coarse minute
    expect( self.series._intervals['minute']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( ('mibucket', 'mrbucket', 'mikey','mrkey') )
    expect( self.series._insert ).args( pipeline, 'mikey', 'value' )
    expect( pipeline.expire ).args( 'mikey', 5*60 )

    expect( pipeline.execute )

    self.series.insert('name', 'val')

  def test_delete(self):
    expect( self.series._client.keys ).args( 'foo:name:*' ).returns( ['k1','k2','k3'] )

    with expect( self.series._client.pipeline ).returns( mock() ) as pipe:
      expect( pipe.delete ).args( 'k1' )
      expect( pipe.delete ).args( 'k2' )
      expect( pipe.delete ).args( 'k3' )
      expect( pipe.execute )

    self.series.delete('name')

  def test_get_for_fine_with_timestamp(self):
    # fine hour
    expect( self.series._intervals['hour']['calc_keys'] ).args(
      'name',3.14).returns( ('hibucket', 'hrbucket', 'hikey','hrkey') )

    expect( self.client.smembers ).args( 'hikey' ).returns( ['1','2','03'] )
    with expect( self.client.pipeline ).returns( mock() ) as pipe:
      expect( self.series._get ).args(pipe, 'hikey:1')
      expect( self.series._get ).args(pipe, 'hikey:2')
      expect( self.series._get ).args(pipe, 'hikey:3')
      expect( pipe.execute ).returns( ['data1','data2','data3'] )

    expect( self.series._process_row ).args( 'data1' ).returns( 'pdata1' )
    expect( self.series._process_row ).args( 'data2' ).returns( 'pdata2' )
    expect( self.series._process_row ).args( 'data3' ).returns( 'pdata3' )

    res = self.series.get( 'name', 'hour', timestamp=3.14 )
    assert_equals( OrderedDict([(60, 'pdata1'), (120, 'pdata2'), (180, 'pdata3')]), res)

  def test_get_for_coarse_without_timestamp(self):
    # coarse minute
    expect( self.series._intervals['minute']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( (7, 'mrbucket', 'mikey','mrkey') )

    expect( self.series._get ).args(self.client, 'mikey').returns( 'data' )
    expect( self.series._process_row ).args( 'data' ).returns( 'pdata' )

    res = self.series.get( 'name', 'minute' )
    assert_equals( OrderedDict([(7*60, 'pdata')]), res)

  def test_get_for_fine_without_timestamp_and_condensed(self):
    # fine hour
    expect( self.series._intervals['hour']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( (7, 'hrbucket', 'hikey','hrkey') )

    expect( self.client.smembers ).args( 'hikey' ).returns( ['1','2','03'] )
    with expect( self.client.pipeline ).returns( mock() ) as pipe:
      expect( self.series._get ).args(pipe, 'hikey:1')
      expect( self.series._get ).args(pipe, 'hikey:2')
      expect( self.series._get ).args(pipe, 'hikey:3')
      expect( pipe.execute ).returns( ['data1','data2','data3'] )

    expect( self.series._process_row ).args( 'data1' ).returns( 'pdata1' )
    expect( self.series._process_row ).args( 'data2' ).returns( 'pdata2' )
    expect( self.series._process_row ).args( 'data3' ).returns( 'pdata3' )
    expect( self.series._condense ).args( 
      OrderedDict([(60, 'pdata1'), (120, 'pdata2'), (180, 'pdata3')]) ).returns(
      'condensed')

    res = self.series.get( 'name', 'hour', condensed=True )
    assert_equals( {25200:'condensed'}, res)
