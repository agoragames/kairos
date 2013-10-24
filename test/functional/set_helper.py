from helper_helper import *
from helper_helper import _time

@unittest.skipUnless( os.environ.get('TEST_SET','true').lower()=='true', 'skipping sets' )
class SetHelper(Chai):

  def setUp(self):
    super(SetHelper,self).setUp()

    self.series = Timeseries(self.client, type='set', prefix='kairos',
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
    self.series.delete_all()

  def tearDown(self):
    self.series.delete_all()

  def test_get(self):
    # 2 hours worth of data. Trim some bits from data resolution to assert
    # proper set behavior
    for t in xrange(1, 7200):
      self.series.insert( 'test', t/15, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    # middle of an interval
    interval = self.series.get( 'test', 'minute', timestamp=_time(100) )
    assert_equals( map(_time, [60]), interval.keys() )
    assert_equals( set([4,5,6,7]), interval[_time(60)] )

    # no matching interval, returns no with empty value list
    interval = self.series.get( 'test', 'minute' )
    assert_equals( 1, len(interval) )
    assert_equals( set(), interval.values()[0] )

    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.get( 'test', 'hour', timestamp=_time(100) )
    assert_equals( 60, len(interval) )
    assert_equals( set([4,5,6,7]), interval[_time(60)] )

    interval = self.series.get( 'test', 'hour', timestamp=_time(100), condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( set(range(1,240)), interval[_time(0)] )

    interval = self.series.get( 'test', 'hour', timestamp=_time(4000), condensed=True )
    assert_equals( 1, len(interval) )
    assert_equals( set(range(240,480)), interval[_time(3600)] )

  def test_series(self):
    # 2 hours worth of data. Trim some bits from data resolution to assert
    # proper set behavior
    for t in xrange(1, 7200):
      self.series.insert( 'test', t/15, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( 'test', 'minute', end=_time(250) )
    assert_equals( map(_time, [0,60,120,180,240]), interval.keys() )
    assert_equals( set([1,2,3]), interval[_time(0)] )
    assert_equals( set([4,5,6,7]), interval[_time(60)] )

    interval = self.series.series( 'test', 'minute', steps=2, end=_time(250) )
    assert_equals( map(_time, [180,240]), interval.keys() )
    assert_equals( set([16,17,18,19]), interval[_time(240)] )

    # with collapse
    interval = self.series.series( 'test', 'minute', end=_time(250), collapse=True )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( set(range(1,20)), interval[_time(0)] )

    ###
    ### with resolution
    ###
    interval = self.series.series( 'test', 'hour', end=_time(250) )
    assert_equals( 1, len(interval) )
    assert_equals( 60, len(interval[_time(0)]) )
    assert_equals( set([1,2,3]), interval[_time(0)][_time(0)] )
    assert_equals( set([4,5,6,7]), interval[_time(0)][_time(60)] )

    # single step, last one
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200) )
    assert_equals( 1, len(interval) )
    assert_equals( set(range(240,480)), interval[_time(3600)] )

    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), steps=2 )
    assert_equals( map(_time, [0,3600]), interval.keys() )
    assert_equals( set(range(1,240)), interval[_time(0)] )
    assert_equals( set(range(240,480)), interval[_time(3600)] )

    # with collapse
    interval = self.series.series( 'test', 'hour', condensed=True, end=_time(4200), steps=2, collapse=True )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( set(range(1,480)), interval[_time(0)] )

  def test_series_joined(self):
    # put some data in the first minutes of each hour for test1, and then for
    # a few more minutes in test2
    self.series.delete('test1')
    self.series.delete('test2')
    for t in xrange(1, 120):
      self.series.insert( 'test1', t/15, timestamp=_time(t) )
      self.series.insert( 'test2', t/15, timestamp=_time(t) )
    for t in xrange(3600, 3720):
      self.series.insert( 'test1', t/15, timestamp=_time(t) )
      self.series.insert( 'test2', t/15, timestamp=_time(t) )
    for t in xrange(120, 240):
      self.series.insert( 'test1', t/15, timestamp=_time(t) )
    for t in xrange(3720, 3840):
      self.series.insert( 'test1', t/15, timestamp=_time(t) )

    ###
    ### no resolution, condensed has no impact
    ###
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250) )
    assert_equals( map(_time,[0,60,120,180,240]), interval.keys() )
    assert_equals( set([1,2,3]), interval[_time(0)] )
    assert_equals( set([4,5,6,7]), interval[_time(60)] )
    assert_equals( set([8,9,10,11]), interval[_time(120)] )
    assert_equals( set([12,13,14,15]), interval[_time(180)] )
    assert_equals( set(), interval[_time(240)] )

    # no matching interval, returns no with empty value list
    interval = self.series.series( ['test1','test2'], 'minute', start=time.time(), steps=2 )
    assert_equals( 2, len(interval) )
    assert_equals( set(), interval.values()[0] )

    # with collapsed
    interval = self.series.series( ['test1','test2'], 'minute', end=_time(250), collapse=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals( set(range(1,16)), interval[_time(0)] )

    ###
    ### with resolution, optionally condensed
    ###
    interval = self.series.series( ['test1','test2'], 'hour', end=_time(250) )
    assert_equals( 1, len(interval) )
    assert_equals( map(_time,[0,60,120,180]), interval[_time(0)].keys() )
    assert_equals( 4, len(interval[_time(0)]) )
    assert_equals( set([1,2,3]), interval[_time(0)][_time(0)] )
    assert_equals( set([4,5,6,7]), interval[_time(0)][_time(60)] )
    assert_equals( set([8,9,10,11]), interval[_time(0)][_time(120)] )
    assert_equals( set([12,13,14,15]), interval[_time(0)][_time(180)] )

    # condensed
    interval = self.series.series( ['test1','test2'], 'hour', end=_time(250), condensed=True )
    assert_equals( [_time(0)], interval.keys() )
    assert_equals( set(range(1,16)), interval[_time(0)] )

    # with collapsed
    interval = self.series.series( ['test1','test2'], 'hour', condensed=True, end=_time(4200), steps=2, collapse=True )
    assert_equals( map(_time, [0]), interval.keys() )
    assert_equals( set(range(1,16)) | set(range(240,256)), interval[_time(0)] )
