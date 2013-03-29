'''
Implementation of functional tests independent of backend
'''
import time
import datetime

from chai import Chai

from kairos.timeseries import *

class SeriesTest(Chai):

  def setUp(self):
    super(SeriesTest,self).setUp()

    self.series = Timeseries(self.client, type='series', prefix='kairos',
      read_func=int, #write_func=str, 
      intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    self.series.delete('test')

  def test_get(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=100 )
    assert_equals( [60], interval.keys() )
    assert_equals( list(range(60,120)), interval[60] )

    # end of an interval
    interval = self.series.get( 'test', 'minute', timestamp=59 )
    assert_equals( [0], interval.keys() )
    assert_equals( list(range(1,60)), interval[0] )
    
    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, len(interval.values()[0]) )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    assert_equals( 60, len(interval) )
    assert_equals( list(range(60,120)), interval[60] )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( list(range(1,3600)), interval[0] )

  def test_series(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', timestamp=250 )
    assert_equals( [0,60,120,180,240], interval.keys() )
    assert_equals( list(range(1,60)), interval[0] )
    assert_equals( list(range(240,300)), interval[240] )
    
    interval = self.series.series( 'test', 'minute', steps=2, timestamp=250 )
    assert_equals( [180,240], interval.keys() )
    assert_equals( list(range(240,300)), interval[240] )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', timestamp=250 )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[0]) )
    assert_equals( list(range(1,60)), interval[0][0] )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200 )
    assert_equals( 1, len(interval) )
    assert_equals( 3600, len(interval[3600]) )
    assert_equals( list(range(3600,7200)), interval[3600] )

    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200, steps=2 )
    assert_equals( [0,3600], interval.keys() )
    assert_equals( 3599, len(interval[0]) )
    assert_equals( 3600, len(interval[3600]) )
    assert_equals( list(range(3600,7200)), interval[3600] )

class HistogramTest(Chai):

  def setUp(self):
    super(HistogramTest,self).setUp()

    self.series = Timeseries(self.client, type='histogram', prefix='kairos',
      read_func=int,
      intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    self.series.delete('test')
  
  def test_get(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t/2, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=100 )
    assert_equals( [60], interval.keys() )
    keys = list(range(30,60))
    assert_equals( keys, interval[60].keys() )
    for k in keys:
      assert_equals( 2, interval[60][k] )

    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, len(interval.values()[0]) )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    keys = list(range(30,60))
    assert_equals( 60, len(interval) )
    assert_equals( keys, interval[60].keys() )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( list(range(0,1800)), interval[0].keys() )

  def test_series(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t/2, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', timestamp=250 )
    assert_equals( [0,60,120,180,240], interval.keys() )
    assert_equals( list(range(0,30)), sorted(interval[0].keys()) )
    assert_equals( 1, interval[0][0] )
    for k in xrange(1,30):
      assert_equals(2, interval[0][k])
    assert_equals( list(range(120,150)), sorted(interval[240].keys()) )
    for k in xrange(120,150):
      assert_equals(2, interval[240][k])
    
    interval = self.series.series( 'test', 'minute', steps=2, timestamp=250 )
    assert_equals( [180,240], interval.keys() )
    assert_equals( list(range(120,150)), sorted(interval[240].keys()) )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', timestamp=250 )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[0]) )
    assert_equals( list(range(0,30)), sorted(interval[0][0].keys()) )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200 )
    assert_equals( 1, len(interval) )
    assert_equals( 1800, len(interval[3600]) )
    assert_equals( list(range(1800,3600)), sorted(interval[3600].keys()) )

    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200, steps=2 )
    assert_equals( [0,3600], interval.keys() )
    assert_equals( 1800, len(interval[0]) )
    assert_equals( 1800, len(interval[3600]) )
    assert_equals( list(range(1800,3600)), sorted(interval[3600].keys()) )

class CountTest(Chai):

  def setUp(self):
    super(CountTest,self).setUp()

    self.series = Timeseries(self.client, type='count', prefix='kairos',
      read_func=int,
      intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    self.series.delete('test')
  
  def test_get(self):
    # 2 hours worth of data
    for t in xrange(1, 7200):
      self.series.insert( 'test', 1, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=100 )
    assert_equals( [60], interval.keys() )
    assert_equals( 60, interval[60] )

    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, interval.values()[0] )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    assert_equals( 60, len(interval) )
    assert_equals( 60, interval[60] )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( 3599, interval[0] )
    
    interval = self.series.get( 'test', 'hour', timestamp=4000, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( 3600, interval[3600] )

  def test_series(self):
    # 2 hours worth of data
    for t in xrange(1, 7200):
      self.series.insert( 'test', 1, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', timestamp=250 )
    assert_equals( [0,60,120,180,240], interval.keys() )
    assert_equals( 59, interval[0] )
    assert_equals( 60, interval[60] )
    
    interval = self.series.series( 'test', 'minute', steps=2, timestamp=250 )
    assert_equals( [180,240], interval.keys() )
    assert_equals( 60, interval[240] )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', timestamp=250 )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[0]) )
    assert_equals( 59, interval[0][0] )
    assert_equals( 60, interval[0][60] )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200 )
    assert_equals( 1, len(interval) )
    assert_equals( 3600, interval[3600] )

    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200, steps=2 )
    assert_equals( [0,3600], interval.keys() )
    assert_equals( 3599, interval[0] )
    assert_equals( 3600, interval[3600] )

class GaugeTest(Chai):

  def setUp(self):
    super(GaugeTest,self).setUp()

    self.series = Timeseries(self.client, type='gauge', prefix='kairos',
      read_func=lambda v: int(v or 0),
      intervals={
        'minute' : {
          'step' : 60,
          'steps' : 5,
        },
        'hour' : {
          'step' : 3600,
          'resolution' : 60,
        }
      } )
    self.series.delete('test')
  
  def test_get(self):
    # 2 hours worth of data
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=100 )
    assert_equals( [60], interval.keys() )
    assert_equals( 119, interval[60] )

    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, interval.values()[0] )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    assert_equals( 60, len(interval) )
    assert_equals( 119, interval[60] )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( range(59,3600,60), interval[0] )
    
    interval = self.series.get( 'test', 'hour', timestamp=4000, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( range(3659,7200,60), interval[3600] )

  def test_series(self):
    # 2 hours worth of data
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=t )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', timestamp=250 )
    assert_equals( [0,60,120,180,240], interval.keys() )
    assert_equals( 59, interval[0] )
    assert_equals( 119, interval[60] )
    
    interval = self.series.series( 'test', 'minute', steps=2, timestamp=250 )
    assert_equals( [180,240], interval.keys() )
    assert_equals( 299, interval[240] )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', timestamp=250 )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[0]) )
    assert_equals( 59, interval[0][0] )
    assert_equals( 119, interval[0][60] )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200 )
    assert_equals( 1, len(interval) )
    assert_equals( range(3659,7200,60), interval[3600] )

    interval = self.series.series( 'test', 'hour', condensed=True, timestamp=4200, steps=2 )
    assert_equals( [0,3600], interval.keys() )
    assert_equals( range(59,3600,60), interval[0] )
    assert_equals( range(3659,7200,60), interval[3600] )
