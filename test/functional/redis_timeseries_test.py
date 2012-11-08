'''
Functional tests for redis timeseries
'''
import time
import datetime

import redis
from chai import Chai

from kairos.timeseries import *


class SeriesTest(Chai):

  def setUp(self):
    super(SeriesTest,self).setUp()

    self.client = redis.Redis('localhost')
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
    # 2 hours worth of data, value is same as timestamp
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
    ### with resolution
    ###
    interval = self.series.get( 'test', 'hour', timestamp=100 )
    assert_equals( 60, len(interval) )
    assert_equals( list(range(60,120)), interval[60] )
    
    interval = self.series.get( 'test', 'hour', timestamp=100, condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( list(range(1,3600)), interval[0] )
    #assert_equals( list(range(60,120)), interval[60] )
