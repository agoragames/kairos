from helper_helper import *
from helper_helper import _time

@unittest.skipUnless( os.environ.get('TEST_GREGORIAN',TEST_ALL).lower()=='true', 'skipping gregorian' )
class GregorianHelper(Chai):
  '''Test that Gregorian data is working right.'''
  def setUp(self):
    super(GregorianHelper,self).setUp()

    self.series = Timeseries(self.client, type='series', prefix='kairos',
      read_func=int, #write_func=str, 
      intervals={
        'daily' : {
          'step' : 'daily',
          'steps' : 5,
        },
        'weekly' : {
          'step' : 'weekly',
          'resolution' : 60,
        },
        'monthly' : {
          'step' : 'monthly',
        },
        'yearly' : {
          'step' : 'yearly',
        }
      } )
    self.series.delete_all()

  def tearDown(self):
    self.series.delete_all()

  def test_insert_multiple_intervals_after(self):
    ts1 = _time(0)
    ts2 = self.series._intervals['weekly']['i_calc'].normalize(ts1, 1)
    ts3 = self.series._intervals['weekly']['i_calc'].normalize(ts1, 2)
    assert_not_equals( ts1, ts2 )

    self.series.insert( 'test', 32, timestamp=ts1, intervals=1 )

    interval_1 = self.series.get( 'test', 'weekly', timestamp=ts1 )
    assert_equals( [32], interval_1[ts1] )

    interval_2 = self.series.get( 'test', 'weekly', timestamp=ts2 )
    assert_equals( [32], interval_2[ts2] )

    self.series.insert( 'test', 42, timestamp=ts1, intervals=2 )

    interval_1 = self.series.get( 'test', 'weekly', timestamp=ts1 )
    assert_equals( [32,42], interval_1[ts1] )
    interval_2 = self.series.get( 'test', 'weekly', timestamp=ts2 )
    assert_equals( [32,42], interval_2[ts2] )
    interval_3 = self.series.get( 'test', 'weekly', timestamp=ts3 )
    assert_equals( [42], interval_3[ts3] )

  def test_insert_multiple_intervals_before(self):
    ts1 = _time(0)
    ts2 = self.series._intervals['weekly']['i_calc'].normalize(ts1, -1)
    ts3 = self.series._intervals['weekly']['i_calc'].normalize(ts1, -2)
    assert_not_equals( ts1, ts2 )

    self.series.insert( 'test', 32, timestamp=ts1, intervals=-1 )

    interval_1 = self.series.get( 'test', 'weekly', timestamp=ts1 )
    assert_equals( [32], interval_1[ts1] )

    interval_2 = self.series.get( 'test', 'weekly', timestamp=ts2 )
    assert_equals( [32], interval_2[ts2] )

    self.series.insert( 'test', 42, timestamp=ts1, intervals=-2 )

    interval_1 = self.series.get( 'test', 'weekly', timestamp=ts1 )
    assert_equals( [32,42], interval_1[ts1] )
    interval_2 = self.series.get( 'test', 'weekly', timestamp=ts2 )
    assert_equals( [32,42], interval_2[ts2] )
    interval_3 = self.series.get( 'test', 'weekly', timestamp=ts3 )
    assert_equals( [42], interval_3[ts3] )

  def test_get(self):
    for day in range(0,365):
      d = datetime(year=2038, month=1, day=1) + timedelta(days=day)
      t = time.mktime( d.timetuple() )
      self.series.insert( 'test', 1, t )
    feb1 = long( time.mktime( datetime(year=2038,month=2,day=1).timetuple() ) )

    data = self.series.get('test', 'daily', timestamp=feb1)
    assert_equals( [1], data[feb1] )

    data = self.series.get('test', 'weekly', timestamp=feb1)
    assert_equals( 7, len(data) )
    assert_equals( [1], data.values()[0] )

    data = self.series.get('test', 'weekly', timestamp=feb1, condensed=True)
    assert_equals( 1, len(data) )
    assert_equals( 7*[1], data.values()[0] )

    data = self.series.get('test', 'monthly', timestamp=feb1)
    assert_equals( 28, len(data[feb1]) )

    data = self.series.get('test', 'yearly', timestamp=feb1)
    assert_equals( 365, len(data.items()[0][1]) )

  def test_series(self):
    for day in range(0,2*365):
      d = datetime(year=2038, month=1, day=1) + timedelta(days=day)
      t = time.mktime( d.timetuple() )
      self.series.insert( 'test', 1, t )

    start = long( time.mktime( datetime(year=2038,month=1,day=1).timetuple() ) )
    end = long( time.mktime( datetime(year=2038,month=12,day=31).timetuple() ) )

    data = self.series.series('test', 'daily', start=start, end=end)
    assert_equals( 365, len(data) )
    assert_equals( [1], data.values()[0] )
    assert_equals( [1], data.values()[-1] )

    data = self.series.series('test', 'weekly', start=start, end=end)
    assert_equals( 53, len(data) )
    assert_equals( 2, len(data.values()[0]) )
    assert_equals( 7, len(data.values()[1]) )
    assert_equals( 6, len(data.values()[-1]) )
    assert_equals( [1], data.values()[0].values()[0] )
    assert_equals( [1], data.values()[-1].values()[0] )

    data = self.series.series('test', 'weekly', start=start, end=end, condensed=True)
    assert_equals( 53, len(data) )
    assert_equals( 2*[1], data.values()[0] )
    assert_equals( 7*[1], data.values()[1] )
    assert_equals( 6*[1], data.values()[-1] )
    
    data = self.series.series('test', 'monthly', start=start, end=end)
    assert_equals( 12, len(data) )
    assert_equals( 31, len(data.values()[0]) ) # jan
    assert_equals( 28, len(data.values()[1]) ) # feb
    assert_equals( 30, len(data.values()[3]) ) # april
    
    data = self.series.series('test', 'yearly', start=start, end=end)
    assert_equals( 1, len(data) )
    assert_equals( 365, len(data.values()[0]) )
    
    data = self.series.series('test', 'yearly', start=start, steps=2)
    assert_equals( 2, len(data) )
    assert_equals( 365, len(data.values()[0]) )
    
    data = self.series.series('test', 'yearly', end=end, steps=2)
    assert_equals( 2, len(data) )
    assert_equals( [], data.values()[0] )
    assert_equals( 365, len(data.values()[1]) )
