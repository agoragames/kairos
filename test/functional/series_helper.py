from helper_helper import *
from helper_helper import _time

@unittest.skipUnless( os.environ.get('TEST_SERIES','true').lower()=='true', 'skipping series' )
class SeriesHelper(Chai):

  def setUp(self):
    super(SeriesHelper,self).setUp()

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

  def test_insert_multiple_intervals_after(self):
    ts1 = _time(0)
    ts2 = self.series._intervals['minute']['i_calc'].normalize(ts1, 1)
    ts3 = self.series._intervals['minute']['i_calc'].normalize(ts1, 2)
    assert_not_equals( ts1, ts2 )

    self.series.insert( 'test', 32, timestamp=ts1, intervals=1 )

    interval_1 = self.series.get( 'test', 'minute', timestamp=ts1 )
    assert_equals( [32], interval_1[ts1] )

    interval_2 = self.series.get( 'test', 'minute', timestamp=ts2 )
    assert_equals( [32], interval_2[ts2] )

    self.series.insert( 'test', 42, timestamp=ts1, intervals=2 )

    interval_1 = self.series.get( 'test', 'minute', timestamp=ts1 )
    assert_equals( [32,42], interval_1[ts1] )
    interval_2 = self.series.get( 'test', 'minute', timestamp=ts2 )
    assert_equals( [32,42], interval_2[ts2] )
    interval_3 = self.series.get( 'test', 'minute', timestamp=ts3 )
    assert_equals( [42], interval_3[ts3] )

  def test_insert_multiple_intervals_before(self):
    ts1 = _time(0)
    ts2 = self.series._intervals['minute']['i_calc'].normalize(ts1, -1)
    ts3 = self.series._intervals['minute']['i_calc'].normalize(ts1, -2)
    assert_not_equals( ts1, ts2 )

    self.series.insert( 'test', 32, timestamp=ts1, intervals=-1 )

    interval_1 = self.series.get( 'test', 'minute', timestamp=ts1 )
    assert_equals( [32], interval_1[ts1] )

    interval_2 = self.series.get( 'test', 'minute', timestamp=ts2 )
    assert_equals( [32], interval_2[ts2] )

    self.series.insert( 'test', 42, timestamp=ts1, intervals=-2 )

    interval_1 = self.series.get( 'test', 'minute', timestamp=ts1 )
    assert_equals( [32,42], interval_1[ts1] )
    interval_2 = self.series.get( 'test', 'minute', timestamp=ts2 )
    assert_equals( [32,42], interval_2[ts2] )
    interval_3 = self.series.get( 'test', 'minute', timestamp=ts3 )
    assert_equals( [42], interval_3[ts3] )

  def test_get(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=_time(100) )
    assert_equals( [_time(60)], interval.keys() )
    assert_equals( list(range(60,120)), interval[_time(60)] )

    # end of an interval
    interval = self.series.get( 'test', 'minute', timestamp=_time(59) )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals( list(range(1,60)), interval[_time(0)] )
    
    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, len(interval.values()[0]) )

    # with transforms
    interval = self.series.get( 'test', 'minute', timestamp=_time(100), transform='count' )
    assert_equals( 60, interval[_time(60)] )
    
    interval = self.series.get( 'test', 'minute', timestamp=_time(100), transform=['min','max'] )
    assert_equals( {'min':60, 'max':119}, interval[_time(60)] )
    
    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=_time(100) )
    assert_equals( 60, len(interval) )
    assert_equals( list(range(60,120)), interval[_time(60)] )
    
    interval = self.series.get( 'test', 'hour', timestamp=_time(100), condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( list(range(1,3600)), interval[_time(0)] )

    # with transforms
    interval = self.series.get( 'test', 'hour', timestamp=_time(100), transform='count' )
    assert_equals( 60, interval[_time(60)] )
    
    interval = self.series.get( 'test', 'hour', timestamp=_time(100), transform=['min','max'], condensed=True )
    assert_equals( {'min':1, 'max':3599}, interval[_time(0)] )

  def test_get_joined(self):
    # put some data in the first minutes of each hour for test1, and then for
    # a few more minutes in test2
    self.series.delete('test1')
    self.series.delete('test2')
    for t in xrange(1, 120):
      self.series.insert( 'test1', t, timestamp=_time(t) )
      self.series.insert( 'test2', t, timestamp=_time(t) )
    for t in xrange(3600, 3720):
      self.series.insert( 'test1', t, timestamp=_time(t) )
      self.series.insert( 'test2', t, timestamp=_time(t) )
    for t in xrange(120, 240):
      self.series.insert( 'test1', t, timestamp=_time(t) )
    for t in xrange(3721, 3840):
      self.series.insert( 'test1', t, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    # interval with 2 series worth of data
    interval = self.series.get( ['test1','test2'], 'minute', timestamp=_time(100) )
    assert_equals( [_time(60)], interval.keys() )
    assert_equals( list(range(60,120))+list(range(60,120)), interval[_time(60)] )

    # interval with 1 series worth of data
    interval = self.series.get( ['test1','test2'], 'minute', timestamp=_time(122) )
    assert_equals( [_time(120)], interval.keys() )
    assert_equals( list(range(120,180)), interval[_time(120)] )

    # no matching interval, returns no with empty value list
    interval = self.series.get( ['test1','test2'], 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( 0, len(interval.values()[0]) )

    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( ['test1','test2'], 'hour', timestamp=_time(100) )
    assert_equals( map(_time,[0,60,120,180]), interval.keys() )
    assert_equals( list(range(1,60))+list(range(1,60)), interval[_time(0)] )
    assert_equals( list(range(60,120))+list(range(60,120)), interval[_time(60)] )
    assert_equals( list(range(120,180)), interval[_time(120)] )
    assert_equals( list(range(180,240)), interval[_time(180)] )

    interval = self.series.get( ['test1','test2'], 'hour', timestamp=_time(100), condensed=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals(
      list(range(1,60))+list(range(1,60))+list(range(60,120))+list(range(60,120))+\
      list(range(120,180))+list(range(180,240)),
      interval[_time(0)] )

    # with transforms
    interval = self.series.get( ['test1','test2'], 'hour', timestamp=_time(100), transform='count' )
    assert_equals( 120, interval[_time(60)] )

    interval = self.series.get( ['test1','test2'], 'hour', timestamp=_time(100), transform=['min','max','count'], condensed=True )
    assert_equals( {'min':1, 'max':239, 'count':358}, interval[_time(0)] )

  def test_series(self):
    # 2 hours worth of data, value is same asV timestamp
    for t in xrange(1, 7200):
      self.series.insert( 'test', t, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', end=_time(250) )
    assert_equals( map(_time,[0,60,120,180,240]), interval.keys() )
    assert_equals( list(range(1,60)), interval[_time(0)] )
    assert_equals( list(range(240,300)), interval[_time(240)] )
    
    interval = self.series.series( 'test', 'minute', steps=2, end=_time(250) )
    assert_equals( map(_time,[180,240]), interval.keys() )
    assert_equals( list(range(240,300)), interval[_time(240)] )

    # with transforms
    interval = self.series.series( 'test', 'minute', end=_time(250), transform=['min','count'] )
    assert_equals( map(_time,[0,60,120,180,240]), interval.keys() )
    assert_equals( {'min':1, 'count':59}, interval[_time(0)] )
    assert_equals( {'min':240, 'count':60}, interval[_time(240)] )

    # with collapsed
    interval = self.series.series( 'test', 'minute', end=_time(250), collapse=True )
    assert_equals( map(_time,[0]), interval.keys() )
    assert_equals( list(range(1,300)), interval[_time(0)] )

    # with transforms and collapsed
    interval = self.series.series( 'test', 'minute', end=_time(250), transform=['min','count'], collapse=True )
    assert_equals( map(_time,[0]), interval.keys() )
    assert_equals( {'min':1, 'count':299}, interval[_time(0)] )
    
    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', end=_time(250) )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[_time(0)]) )
    assert_equals( list(range(1,60)), interval[_time(0)][_time(0)] )

    interval = self.series.series( 'test', 'hour', end=_time(250), transform=['count','max'] )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[_time(0)]) )
    assert_equals( {'max':59, 'count':59}, interval[_time(0)][_time(0)] )

    # single step, last one    
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200) )
    assert_equals( 1, len(interval) )
    assert_equals( 3600, len(interval[_time(3600)]) )
    assert_equals( list(range(3600,7200)), interval[_time(3600)] )

    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), steps=2 )
    assert_equals( map(_time, [0,3600]), interval.keys() )
    assert_equals( 3599, len(interval[_time(0)]) )
    assert_equals( 3600, len(interval[_time(3600)]) )
    assert_equals( list(range(3600,7200)), interval[_time(3600)] )
    
    # with transforms
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), transform=['min','max'] )
    assert_equals( 1, len(interval) )
    assert_equals( {'min':3600, 'max':7199}, interval[_time(3600)] )

    # with collapsed
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), steps=2, collapse=True )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( 7199, len(interval[_time(0)]) )
    assert_equals( list(range(1,7200)), interval[_time(0)] )

    # with transforms and collapsed
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), steps=2, collapse=True, transform=['min','count','max'] )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( {'min':1, 'max':7199, 'count':7199}, interval[_time(0)] )

  def test_series_joined(self):
    # put some data in the first minutes of each hour for test1, and then for
    # a few more minutes in test2
    self.series.delete('test1')
    self.series.delete('test2')
    for t in xrange(1, 120):
      self.series.insert( 'test1', t, timestamp=_time(t) )
      self.series.insert( 'test2', t, timestamp=_time(t) )
    for t in xrange(3600, 3720):
      self.series.insert( 'test1', t, timestamp=_time(t) )
      self.series.insert( 'test2', t, timestamp=_time(t) )
    for t in xrange(120, 240):
      self.series.insert( 'test1', t, timestamp=_time(t) )
    for t in xrange(3720, 3840):
      self.series.insert( 'test1', t, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250) )
    assert_equals( map(_time,[0,60,120,180,240]), interval.keys() )
    assert_equals( list(range(1,60))+list(range(1,60)), interval[_time(0)] )
    assert_equals( list(range(60,120))+list(range(60,120)), interval[_time(60)] )
    assert_equals( list(range(120,180)), interval[_time(120)] )
    assert_equals( list(range(180,240)), interval[_time(180)] )
    assert_equals( [], interval[_time(240)] )

    # no matching interval, returns no with empty value list
    interval = self.series.series( ['test1','test2'], 'minute', start=time.time(), steps=2 )
    assert_equals( 2, len(interval) )
    assert_equals( [], interval.values()[0] )

    # with transforms
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250), transform=['min','count'] )
    assert_equals( map(_time,[0,60,120,180,240]), interval.keys() )
    assert_equals( {'min':1, 'count':118}, interval[_time(0)] )
    assert_equals( {'min':60, 'count':120}, interval[_time(60)] )
    assert_equals( {'min':120, 'count':60}, interval[_time(120)] )
    assert_equals( {'min':180, 'count':60}, interval[_time(180)] )
    assert_equals( {'min':0, 'count':0}, interval[_time(240)] )

    # with collapsed
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250), collapse=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals(
      list(range(1,60))+list(range(1,60))+list(range(60,120))+list(range(60,120))+\
      list(range(120,180))+list(range(180,240)),
      interval[_time(0)] )

    # with tranforms and collapsed
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250), transform=['min','max', 'count'], collapse=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals( {'min':1, 'max':239, 'count':358}, interval[_time(0)] )

    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.series( ['test1','test2'], 'hour', end=_time(250) )
    assert_equals( 1, len(interval) )
    assert_equals( map(_time,[0,60,120,180]), interval[_time(0)].keys() )
    assert_equals( 4, len(interval[_time(0)]) )
    assert_equals( list(range(1,60))+list(range(1,60)), interval[_time(0)][_time(0)] )
    assert_equals( list(range(60,120))+list(range(60,120)), interval[_time(0)][_time(60)] )
    assert_equals( list(range(120,180)), interval[_time(0)][_time(120)] )
    assert_equals( list(range(180,240)), interval[_time(0)][_time(180)] )

    # condensed
    interval = self.series.series( ['test1','test2'], 'hour', end=_time(250), condensed=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals(
      list(range(1,60))+list(range(1,60))+list(range(60,120))+list(range(60,120))+\
      list(range(120,180))+list(range(180,240)),
      interval[_time(0)] )

    # with collapsed
    interval = self.series.series( ['test1','test2'], 'hour', condensed=True, end=_time(4200), steps=2, collapse=True )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals(
      list(range(1,60))+list(range(1,60))+list(range(60,120))+list(range(60,120))+\
      list(range(120,180))+list(range(180,240))+\
      list(range(3600,3660))+list(range(3600,3660))+list(range(3660,3720))+list(range(3660,3720))+\
      list(range(3720,3780))+list(range(3780,3840)),
      interval[_time(0)] )

    # with transforms collapsed
    interval = self.series.series( ['test1','test2'], 'hour', condensed=True, end=_time(4200), steps=2, collapse=True, transform=['min','max','count'] )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( {'min':1,'max':3839,'count':718}, interval[_time(0)] )
