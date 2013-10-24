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
        },
        'bulk-hour' : {
          'step' : 3600,
        }
      } )
    self.series.delete_all()

  def tearDown(self):
    self.series.delete_all()

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
    self.series.insert( 'test', 32, timestamp=_time(60) )
    self.series.insert( 'test', 32, timestamp=_time(600) )

    res = self.series.properties('test')
    assert_equals( _time(0), res['minute']['first'] )
    assert_equals( _time(600), res['minute']['last'] )
    assert_equals( _time(0), res['hour']['first'] )
    assert_equals( _time(0), res['hour']['last'] )

    self.series.delete('test')

  def test_iterate(self):
    self.series.insert( 'test', 32, timestamp=_time(0) )
    self.series.insert( 'test', 42, timestamp=_time(60) )
    self.series.insert( 'test', 52, timestamp=_time(600) )

    # There should be a result for every possible step between first and last
    res = list(self.series.iterate('test','minute'))
    assert_equals( 11, len(res) )
    assert_equals( (_time(0),[32]), res[0] )
    assert_equals( (_time(60),[42]), res[1] )
    assert_equals( (_time(120),[]), res[2] )
    assert_equals( (_time(600),[52]), res[-1] )

    # With resolutions, there should be a result only for where there's data      
    res = list(self.series.iterate('test','hour'))
    assert_equals( 3, len(res) )
    assert_equals( (_time(0),[32]), res[0] )
    assert_equals( (_time(60),[42]), res[1] )
    assert_equals( (_time(600),[52]), res[2] )

    # Without resolutions, there should be a single result
    res = list(self.series.iterate('test','bulk-hour'))
    assert_equals( 1, len(res) )
    assert_equals( (_time(0),[32,42,52]), res[0] )

    self.series.delete('test')
