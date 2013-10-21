from helper_helper import *
from helper_helper import _time

@unittest.skipUnless( os.environ.get('TEST_API','true').lower()=='true', 'skipping api' )
class ApiHelper(Chai):

  def setUp(self):
    super(ApiHelper,self).setUp()

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

  def test_list(self):
    self.series.insert( 'test', 32, timestamp=_time(0) )
    self.series.insert( 'test1', 32, timestamp=_time(0) )
    self.series.insert( 'test2', 32, timestamp=_time(0) )
    self.series.insert( 'test', 32, timestamp=_time(0) )

    res = sorted(self.series.list())
    assert_equals( ['test', 'test1', 'test2'], res )

    self.series.delete('test')
    self.series.delete('test1')
    self.series.delete('test2')

  def test_properties(self):
    self.series.insert( 'test', 32, timestamp=_time(0) )
    self.series.insert( 'test1', 32, timestamp=_time(0) )
    self.series.insert( 'test2', 32, timestamp=_time(0) )
    self.series.insert( 'test', 32, timestamp=_time(60) )
    self.series.insert( 'test', 32, timestamp=_time(600) )

    try:
      res = self.series.properties('test')
      assert_equals( _time(0), res['minute']['first'] )
      assert_equals( _time(600), res['minute']['last'] )
      assert_equals( _time(0), res['hour']['first'] )
      assert_equals( _time(0), res['hour']['last'] )

    finally:
      self.series.delete('test')
      self.series.delete('test1')
      self.series.delete('test2')
