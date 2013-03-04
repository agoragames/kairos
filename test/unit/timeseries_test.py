'''
Unit tests for timeseries
'''

import time

from chai import Chai

from kairos.timeseries import *
from kairos.exceptions import *

# HACK for ease of testing
class SortedDict(dict):
  def iteritems(self):
    for key in sorted(self.keys()):
      yield (key, self[key])

class TimeseriesTest(Chai):

  def setUp(self):
    super(TimeseriesTest,self).setUp()

    self.client = mock()
    self.series = Timeseries(self.client, prefix='foo', read_func=mock(), 
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

  def test_init_with_relative_time_args(self):
    t = Timeseries(self.client, type='series', prefix='foo', read_func='get', 
      write_func='set', intervals={
        'hour' : {
          'step' : '1h',
          'steps' : 5,
        },
        'year' : {
          'step' : '1y',
          'resolution' : '1d',
        }
      } )
    assert_equals( self.client, t._client )
    assert_equals( 'get', t._read_func )
    assert_equals( 'set', t._write_func )
    assert_equals( 'foo:', t._prefix )

    assert_equals( 5*60*60, t._intervals['hour']['expire'] )
    assert_false( t._intervals['year']['expire'] )
    assert_true( t._intervals['hour']['coarse'] )
    assert_false( t._intervals['year']['coarse'] )

    now = time.time()

    i = r = int(now/3600)
    assert_equals( (i, r, 'foo:test:hour:%s'%(i), 'foo:test:hour:%s:%s'%(i,r)), 
      t._intervals['hour']['calc_keys']('test', now) )
    
    i = long(now/(60*60*24*365))
    r = long(now/(60*60*24))
    assert_equals( (i, r, 'foo:test:year:%s'%(i), 'foo:test:year:%s:%s'%(i,r)), 
      t._intervals['year']['calc_keys']('test', now) )

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
    self.series._intervals['minute']['coarse'] = False
    self.series._intervals['hour']['expire'] = 7*3600 # 7 hours

    # not coarse hour
    expect( self.series._intervals['hour']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( ('hibucket', 'hrbucket', 'hikey','hrkey') )
    expect( pipeline.sadd ).args( 'hikey', 'hrbucket' )
    expect( self.series._insert ).args( pipeline, 'hrkey', 'value' )
    expect( pipeline.expire ).args( 'hikey', 7*3600 )
    expect( pipeline.expire ).args( 'hrkey', 7*3600 )

    # coarse minute
    expect( self.series._intervals['minute']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( ('mibucket', 'mrbucket', 'mikey','mrkey') )
    expect( pipeline.sadd ).args( 'mikey', 'mrbucket' )
    expect( self.series._insert ).args( pipeline, 'mrkey', 'value' )
    expect( pipeline.expire ).args( 'mikey', 5*60 )
    expect( pipeline.expire ).args( 'mrkey', 5*60 )

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

  def test_get_for_fine_with_timestamp_and_transform(self):
    # fine hour
    expect( self.series._intervals['hour']['calc_keys'] ).args(
      'name',3.14).returns( ('hibucket', 'hrbucket', 'hikey','hrkey') )

    expect( self.client.smembers ).args( 'hikey' ).returns( ['1','2','03'] )
    with expect( self.client.pipeline ).returns( mock() ) as pipe:
      expect( self.series._get ).args(pipe, 'hikey:1')
      expect( self.series._get ).args(pipe, 'hikey:2')
      expect( self.series._get ).args(pipe, 'hikey:3')
      expect( pipe.execute ).returns( ['data1','data2','data3'] )

    expect( self.series._process_row ).args( 'data1' ).returns( [1,2,3] )
    expect( self.series._process_row ).args( 'data2' ).returns( [4,5,6] )
    expect( self.series._process_row ).args( 'data3' ).returns( [7,8,9] )
    expect( self.series._transform ).args( [1,2,3], 'max' ).returns( 3 )
    expect( self.series._transform ).args( [4,5,6], 'max' ).returns( 6 )
    expect( self.series._transform ).args( [7,8,9], 'max' ).returns( 9 )

    res = self.series.get( 'name', 'hour', timestamp=3.14, transform='max' )
    assert_equals( OrderedDict([(60, 3), (120, 6), (180, 9)]), res)

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

  def test_get_for_fine_without_timestamp_and_condensed_and_transform(self):
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
      [1,2,3])
    expect( self.series._transform ).args([1,2,3], 'max').returns( 3 )

    res = self.series.get( 'name', 'hour', condensed=True, transform='max' )
    assert_equals( {25200:3}, res)

  def test_get_for_coarse_without_timestamp_and_transform(self):
    # coarse minute
    expect( self.series._intervals['minute']['calc_keys'] ).args(
      'name', almost_equals(time.time(),2) ).returns( (7, 'mrbucket', 'mikey','mrkey') )

    expect( self.series._get ).args(self.client, 'mikey').returns( 'data' )
    expect( self.series._process_row ).args( 'data' ).returns( [1,2,3] )
    expect( self.series._transform ).args( [1,2,3], 'max' ).returns( 3 )

    res = self.series.get( 'name', 'minute', transform='max' )
    assert_equals( OrderedDict([(7*60, 3)]), res)

  def test_get_raises_unknowninterval(self):
    assert_raises( UnknownInterval, self.series.get, 'name', 'lightyear' )

  def test_series_for_fine_when_steps_not_condensed(self):
    end_bucket = int( time.time()/3600 )
    start_bucket = end_bucket - 2   # -3+1

    # fetch all of the buckets which store resolution data for each of the
    # intervals in the series. in the real world, the resolution buckets
    # returned would have much larger numbers and be consistent with the
    # interval buckets
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+1) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+2) )
      expect( pipe.execute ).returns( [('1','2','03'),('4','05','6'),('7','8','9')] )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:1'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:2'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:3'%(start_bucket))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3'] )
      expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
      expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
      expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:4'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:5'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:6'%(start_bucket+1))
      expect( pipe.execute ).returns( ['row4', 'row5', 'row6'] )
      expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
      expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
      expect( self.series._process_row ).args( 'row6' ).returns( 'prow6' )
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:7'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:8'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:9'%(start_bucket+2))
      expect( pipe.execute ).returns( ['row7', 'row8', 'row9'] ) 
      expect( self.series._process_row ).args( 'row7' ).returns( 'prow7' )
      expect( self.series._process_row ).args( 'row8' ).returns( 'prow8' )
      expect( self.series._process_row ).args( 'row9' ).returns( 'prow9' )

    res = self.series.series('name', 'hour', steps=3)
    assert_equals( OrderedDict([(60, 'prow1'), (120, 'prow2'), (180, 'prow3')]), 
      res[start_bucket*3600] )
    assert_equals( OrderedDict([(240, 'prow4'), (300, 'prow5'), (360, 'prow6')]), 
      res[(start_bucket+1)*3600] )
    assert_equals( OrderedDict([(420, 'prow7'), (480, 'prow8'), (540, 'prow9')]), 
      res[(start_bucket+2)*3600] )

  def test_series_for_fine_when_steps_not_condensed_and_transform(self):
    end_bucket = int( time.time()/3600 )
    start_bucket = end_bucket - 2   # -3+1

    # fetch all of the buckets which store resolution data for each of the
    # intervals in the series. in the real world, the resolution buckets
    # returned would have much larger numbers and be consistent with the
    # interval buckets
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+1) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+2) )
      expect( pipe.execute ).returns( [('1','2','03'),('4','05','6'),('7','8','9')] )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:1'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:2'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:3'%(start_bucket))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3'] )
      expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
      expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
      expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )
      expect( self.series._transform ).args( 'prow1', 'max' ).returns( 'max1' )
      expect( self.series._transform ).args( 'prow2', 'max' ).returns( 'max2' )
      expect( self.series._transform ).args( 'prow3', 'max' ).returns( 'max3' )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:4'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:5'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:6'%(start_bucket+1))
      expect( pipe.execute ).returns( ['row4', 'row5', 'row6'] )
      expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
      expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
      expect( self.series._process_row ).args( 'row6' ).returns( 'prow6' )
      expect( self.series._transform ).args( 'prow4', 'max' ).returns( 'max4' )
      expect( self.series._transform ).args( 'prow5', 'max' ).returns( 'max5' )
      expect( self.series._transform ).args( 'prow6', 'max' ).returns( 'max6' )
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:7'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:8'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:9'%(start_bucket+2))
      expect( pipe.execute ).returns( ['row7', 'row8', 'row9'] ) 
      expect( self.series._process_row ).args( 'row7' ).returns( 'prow7' )
      expect( self.series._process_row ).args( 'row8' ).returns( 'prow8' )
      expect( self.series._process_row ).args( 'row9' ).returns( 'prow9' )
      expect( self.series._transform ).args( 'prow7', 'max' ).returns( 'max7' )
      expect( self.series._transform ).args( 'prow8', 'max' ).returns( 'max8' )
      expect( self.series._transform ).args( 'prow9', 'max' ).returns( 'max9' )

    res = self.series.series('name', 'hour', steps=3, transform='max')
    assert_equals( OrderedDict([(60, 'max1'), (120, 'max2'), (180, 'max3')]), 
      res[start_bucket*3600] )
    assert_equals( OrderedDict([(240, 'max4'), (300, 'max5'), (360, 'max6')]), 
      res[(start_bucket+1)*3600] )
    assert_equals( OrderedDict([(420, 'max7'), (480, 'max8'), (540, 'max9')]), 
      res[(start_bucket+2)*3600] )

  def test_series_for_fine_when_steps_and_condensed(self):
    end_bucket = int( time.time()/3600 )
    start_bucket = end_bucket - 2   # -3+1

    # fetch all of the buckets which store resolution data for each of the
    # intervals in the series. in the real world, the resolution buckets
    # returned would have much larger numbers and be consistent with the
    # interval buckets
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+1) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+2) )
      expect( pipe.execute ).returns( [('1','2','03'),('4','05','6'),('7','8','9')] )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:1'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:2'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:3'%(start_bucket))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3'] )
      expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
      expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
      expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:4'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:5'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:6'%(start_bucket+1))
      expect( pipe.execute ).returns( ['row4', 'row5', 'row6'] )
      expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
      expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
      expect( self.series._process_row ).args( 'row6' ).returns( 'prow6' )
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:7'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:8'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:9'%(start_bucket+2))
      expect( pipe.execute ).returns( ['row7', 'row8', 'row9'] ) 
      expect( self.series._process_row ).args( 'row7' ).returns( 'prow7' )
      expect( self.series._process_row ).args( 'row8' ).returns( 'prow8' )
      expect( self.series._process_row ).args( 'row9' ).returns( 'prow9' )

    expect( self.series._condense ).args(
      OrderedDict([(60, 'prow1'), (120, 'prow2'), (180, 'prow3')]) ).returns('c1')
    expect( self.series._condense ).args(
      OrderedDict([(240, 'prow4'), (300, 'prow5'), (360, 'prow6')]) ).returns('c2')
    expect( self.series._condense ).args(
      OrderedDict([(420, 'prow7'), (480, 'prow8'), (540, 'prow9')]) ).returns('c3')

    res = self.series.series('name', 'hour', steps=3, condensed=True)
    assert_equals('c1', res[start_bucket*3600] )
    assert_equals('c2', res[(start_bucket+1)*3600] )
    assert_equals('c3', res[(start_bucket+2)*3600] )

  def test_series_for_fine_when_steps_and_condensed_and_transform(self):
    end_bucket = int( time.time()/3600 )
    start_bucket = end_bucket - 2   # -3+1

    # fetch all of the buckets which store resolution data for each of the
    # intervals in the series. in the real world, the resolution buckets
    # returned would have much larger numbers and be consistent with the
    # interval buckets
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+1) )
      expect( pipe.smembers ).args( 'foo:name:hour:%s'%(start_bucket+2) )
      expect( pipe.execute ).returns( [('1','2','03'),('4','05','6'),('7','8','9')] )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:1'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:2'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:3'%(start_bucket))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3'] )
      expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
      expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
      expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )

    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:4'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:5'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:6'%(start_bucket+1))
      expect( pipe.execute ).returns( ['row4', 'row5', 'row6'] )
      expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
      expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
      expect( self.series._process_row ).args( 'row6' ).returns( 'prow6' )
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:7'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:8'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:hour:%s:9'%(start_bucket+2))
      expect( pipe.execute ).returns( ['row7', 'row8', 'row9'] ) 
      expect( self.series._process_row ).args( 'row7' ).returns( 'prow7' )
      expect( self.series._process_row ).args( 'row8' ).returns( 'prow8' )
      expect( self.series._process_row ).args( 'row9' ).returns( 'prow9' )

    expect( self.series._condense ).args(
      OrderedDict([(60, 'prow1'), (120, 'prow2'), (180, 'prow3')]) ).returns('c1')
    expect( self.series._condense ).args(
      OrderedDict([(240, 'prow4'), (300, 'prow5'), (360, 'prow6')]) ).returns('c2')
    expect( self.series._condense ).args(
      OrderedDict([(420, 'prow7'), (480, 'prow8'), (540, 'prow9')]) ).returns('c3')
    
    expect( self.series._transform ).args( 'c1', 'max' ).returns( 'max1' )
    expect( self.series._transform ).args( 'c2', 'max' ).returns( 'max2' )
    expect( self.series._transform ).args( 'c3', 'max' ).returns( 'max3' )

    res = self.series.series('name', 'hour', steps=3, condensed=True, transform='max')
    assert_equals('max1', res[start_bucket*3600] )
    assert_equals('max2', res[(start_bucket+1)*3600] )
    assert_equals('max3', res[(start_bucket+2)*3600] )

  def test_series_for_coarse_when_no_steps(self):
    end_bucket = int( time.time()/60 )
    start_bucket = end_bucket - 4   # -5+1
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+3))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+4))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3', 'row4', 'row5'] )
    
    expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
    expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
    expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )
    expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
    expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
    
    res = self.series.series('name', 'minute')
    assert_equals( res, OrderedDict([
      (start_bucket*60, 'prow1'),
      ((start_bucket+1)*60, 'prow2'),
      ((start_bucket+2)*60, 'prow3'),
      ((start_bucket+3)*60, 'prow4'),
      ((start_bucket+4)*60, 'prow5'),
    ]) )

  def test_series_for_coarse_when_no_steps_and_transform(self):
    end_bucket = int( time.time()/60 )
    start_bucket = end_bucket - 4   # -5+1
    
    with expect( self.client.pipeline ).returns(mock()) as pipe:
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+1))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+2))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+3))
      expect( self.series._get ).args(pipe, 'foo:name:minute:%s'%(start_bucket+4))
      expect( pipe.execute ).returns( ['row1', 'row2', 'row3', 'row4', 'row5'] )
    
    expect( self.series._process_row ).args( 'row1' ).returns( 'prow1' )
    expect( self.series._process_row ).args( 'row2' ).returns( 'prow2' )
    expect( self.series._process_row ).args( 'row3' ).returns( 'prow3' )
    expect( self.series._process_row ).args( 'row4' ).returns( 'prow4' )
    expect( self.series._process_row ).args( 'row5' ).returns( 'prow5' )
    expect( self.series._transform ).args( 'prow1', 'max' ).returns( 'max1' )
    expect( self.series._transform ).args( 'prow2', 'max' ).returns( 'max2' )
    expect( self.series._transform ).args( 'prow3', 'max' ).returns( 'max3' )
    expect( self.series._transform ).args( 'prow4', 'max' ).returns( 'max4' )
    expect( self.series._transform ).args( 'prow5', 'max' ).returns( 'max5' )
    
    res = self.series.series('name', 'minute', transform='max')
    assert_equals( res, OrderedDict([
      (start_bucket*60, 'max1'),
      ((start_bucket+1)*60, 'max2'),
      ((start_bucket+2)*60, 'max3'),
      ((start_bucket+3)*60, 'max4'),
      ((start_bucket+4)*60, 'max5'),
    ]) )

  def test_series_raises_unknowninterval(self):
    assert_raises( UnknownInterval, self.series.series, 'name', 'lightyear' )
  
  def test__insert(self):
    assert_raises( NotImplementedError, self.series._insert, 'handle', 'key', 'value' )

  def test__get(self):
    assert_raises( NotImplementedError, self.series._get, 'handle', 'key' )

  def test__process_row(self):
    assert_raises( NotImplementedError, self.series._process_row, 'data' )

  def test__condense(self):
    assert_raises( NotImplementedError, self.series._condense, 'data' )

  def test__transform(self):
    assert_raises( NotImplementedError, self.series._transform, 'data', 'max' )


class SeriesTest(Chai):

  def setUp(self):
    super(SeriesTest,self).setUp()

    self.client = mock()
    self.series = Series(self.client, type='series', prefix='foo', read_func=mock(), 
      write_func=mock(), intervals={})

  def test_insert(self):
    handle = mock()
    expect( handle.rpush ).args( 'k', 'v' )
    self.series._insert( handle, 'k', 'v' )

  def test_get(self):
    handle = mock()
    expect( handle.lrange ).args( 'k', 0, -1 ).returns('data')
    assert_equals( 'data', self.series._get(handle, 'k') )

  def test_process_row(self):
    expect( self.series._read_func ).args( 'd1' ).returns( 'p1' )
    expect( self.series._read_func ).args( 'd2' ).returns( 'p2' )

    assert_equals( ['p1','p2'], self.series._process_row(['d1','d2']) )

    self.series._read_func = None
    assert_equals( ['d1','d2'], self.series._process_row(['d1','d2']) )

  def test_condense(self):
    x = OrderedDict([('a', [1,2]), ('b', [3,4])])
    assert_equals( [1,2,3,4], self.series._condense(x) )

    assert_equals( [], self.series._condense({}) )

  def test_transform(self):
    assert_equals( 2, self.series._transform([1,2,3], 'mean') )
    assert_equals( 3, self.series._transform([1,2,3], 'count') )
    assert_equals( 1, self.series._transform([1,2,3], 'min') )
    assert_equals( 3, self.series._transform([1,2,3], 'max') )
    assert_equals( 6, self.series._transform([1,2,3], 'sum') )

    cable = mock()
    with expect(cable).args([1,2,3]).returns(5):
      assert_equals( 5, self.series._transform([1,2,3], cable) )

class HistogramTest(Chai):

  def setUp(self):
    super(HistogramTest,self).setUp()

    self.client = mock()
    self.series = Timeseries(self.client, type='histogram', prefix='foo', read_func=mock(), 
      write_func=mock(), intervals={})

  def test_insert(self):
    handle = mock()
    expect( handle.hincrby ).args( 'k', 'v', 1 )
    self.series._insert( handle, 'k', 'v' )

  def test_get(self):
    handle = mock()
    expect( handle.hgetall ).args( 'k' ).returns('data')
    assert_equals( 'data', self.series._get(handle, 'k') )

  def test_process_row(self):
    expect( self.series._read_func ).args( 'v1' ).returns( 'p1' )
    expect( self.series._read_func ).args( 'v2' ).returns( 'p2' )

    assert_equals( {'p1':3,'p2':5}, self.series._process_row({'v1':'3','v2':'5'}) )

    self.series._read_func = None
    assert_equals( {'v1':3,'v2':5}, self.series._process_row({'v1':'3','v2':'5'}) )

  def test_condense(self):
    x = OrderedDict([('a', {'x':1, 'y':2}), ('b', {'x':3, 'y':4})])
    assert_equals( {'x':4,'y':6}, self.series._condense(x) )

    assert_equals( {}, self.series._condense({}) )

  def test_transform(self):
    d = { 1.0: 4, 3.0: 2 }

    assert_almost_equals( 1.667, self.series._transform(d, 'mean'), 3 )
    assert_equals( 6, self.series._transform(d, 'count') )
    assert_equals( 1.0, self.series._transform(d, 'min') )
    assert_equals( 3.0, self.series._transform(d, 'max') )
    assert_equals( 10, self.series._transform(d, 'sum') )

    cable = mock()
    with expect(cable).args([1.0,1.0,1.0,1.0,3.0,3.0]).returns(84):
      assert_equals( 84, self.series._transform(d, cable) )

class CountTest(Chai):

  def setUp(self):
    super(CountTest,self).setUp()

    self.client = mock()
    self.series = Timeseries(self.client, type='count', prefix='foo', read_func=mock(), 
      write_func=mock(), intervals={})

  def test_insert_1(self):
    handle = mock()
    expect( handle.incr ).args( 'k' )
    self.series._insert( handle, 'k', 1 )

  def test_insert_float(self):
    handle = mock()
    expect( handle.incrbyfloat ).args( 'k', 3.14 )
    self.series._insert( handle, 'k', 3.14 )

  def test_insert_other(self):
    handle = mock()
    expect( handle.incrby ).args( 'k', 7 )
    self.series._insert( handle, 'k', 7 )

  def test_get(self):
    handle = mock()
    expect( handle.get ).args( 'k' ).returns('data')
    assert_equals( 'data', self.series._get(handle, 'k') )

  def test_process_row(self):
    assert_equals( 42, self.series._process_row('42') )
    assert_equals( 0, self.series._process_row('') )

  def test_condense(self):
    x = OrderedDict([('a', 1), ('b', 3)])
    assert_equals( 4, self.series._condense(x) )

    assert_equals( 0, self.series._condense({}) )

  def test_transform(self):
    cable = mock()
    with expect(cable).args(5).returns(84):
      assert_equals( 84, self.series._transform(5, cable) )

    assert_equals( 32, self.series._transform(32, 'max') )
