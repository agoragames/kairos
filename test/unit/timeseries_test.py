'''
Functional tests for timeseries core
'''
import time
from datetime import datetime

from kairos.timeseries import *
from chai import Chai

class UrlTest(Chai):
  def test_bad_url(self):
    with assert_raises(ImportError):
      Timeseries("noop://foo/bar")

class RelativeTimeTest(Chai):

  def test_step_size(self):
    DAY = 60*60*24
    rt = RelativeTime( DAY )
    assert_equals( DAY, rt.step_size() )
    assert_equals( DAY, rt.step_size(0, 0) )

    assert_equals( DAY, rt.step_size(0, 0) )
    assert_equals( DAY, rt.step_size(0, DAY/2) )
    assert_equals( DAY, rt.step_size(0, DAY-1) )
    assert_equals( 2*DAY, rt.step_size(0, DAY) )
    assert_equals( 2*DAY, rt.step_size(0, DAY+(60*60*1)) )
    assert_equals( 3*DAY, rt.step_size(0, 2*DAY+1) )
    assert_equals( 2*DAY, rt.step_size(DAY+1, 2*DAY) )

  def test_ttl(self):
    DAY = 60*60*24
    rt = RelativeTime( DAY )
    assert_equals( 3*DAY, rt.ttl( 3 ) )
    assert_equals( 3*DAY, rt.ttl( 3, relative_time=time.time() ) )
    assert_equals( 4*DAY, rt.ttl( 3, relative_time=time.time()+DAY ) )
    assert_equals( 8*DAY, rt.ttl( 3, relative_time=time.time()+(5*DAY) ) )
    assert_equals( 2*DAY, rt.ttl( 3, relative_time=time.time()-DAY ) )
    assert_equals( DAY, rt.ttl( 3, relative_time=time.time()-2*DAY ) )
    assert_equals( 0, rt.ttl( 3, relative_time=time.time()-3*DAY ) )

class GregorianTimeTest(Chai):

  def test_buckets(self):
    gt = GregorianTime( 'daily' )
    buckets = gt.buckets( 0, 60*60*24*42 )
    assert_equals( buckets[:3], [19700101, 19700102, 19700103] )
    assert_equals( buckets[-3:], [19700209, 19700210, 19700211] )

    gt = GregorianTime( 'weekly' )
    buckets = gt.buckets( 0, 60*60*24*25 )
    assert_equals( buckets, [197000, 197001, 197002, 197003] )

    gt = GregorianTime( 'monthly' )
    buckets = gt.buckets( 0, 60*60*24*70 )
    assert_equals( buckets, [197001, 197002, 197003] )

    gt = GregorianTime( 'yearly' )
    buckets = gt.buckets( 0, 60*60*24*800 )
    assert_equals( buckets, [1970, 1971, 1972] )

  def test_step_size(self):
    DAY = 60*60*24
    gtd = GregorianTime( 'daily' )
    gtm = GregorianTime( 'monthly' )
    gty = GregorianTime( 'yearly' )

    # leap year
    t0 = time.mktime( datetime(year=2012, month=1, day=1).timetuple() )
    t1 = time.mktime( datetime(year=2012, month=1, day=5).timetuple() )
    t2 = time.mktime( datetime(year=2012, month=2, day=13).timetuple() )
    t3 = time.mktime( datetime(year=2012, month=2, day=29).timetuple() )
    t4 = time.mktime( datetime(year=2012, month=3, day=5).timetuple() )

    assert_equals( DAY, gtd.step_size(t0) )
    assert_equals( 31*DAY, gtm.step_size(t0) )
    assert_equals( 366*DAY, gty.step_size(t0) )

    assert_equals( DAY, gtd.step_size(t2) )
    assert_equals( 31*DAY, gtm.step_size(t0, t1) )
    assert_equals( 60*DAY, gtm.step_size(t1, t2) )
    assert_equals( 29*DAY, gtm.step_size(t2, t3) )
    assert_equals( 91*DAY, gtm.step_size(t1, t4) )
    assert_equals( 60*DAY, gtm.step_size(t2, t4) )

    # not-leap year
    t0 = time.mktime( datetime(year=2013, month=1, day=1).timetuple() )
    t1 = time.mktime( datetime(year=2013, month=1, day=5).timetuple() )
    t2 = time.mktime( datetime(year=2013, month=2, day=13).timetuple() )
    t3 = time.mktime( datetime(year=2013, month=2, day=28).timetuple() )
    t4 = time.mktime( datetime(year=2013, month=3, day=5).timetuple() )

    assert_equals( DAY, gtd.step_size(t0) )
    assert_equals( 31*DAY, gtm.step_size(t0) )
    assert_equals( 365*DAY, gty.step_size(t0) )

    assert_equals( DAY, gtd.step_size(t2) )
    assert_equals( 31*DAY, gtm.step_size(t0, t1) )
    assert_equals( 59*DAY, gtm.step_size(t1, t2) )
    assert_equals( 28*DAY, gtm.step_size(t2, t3) )
    assert_equals( 90*DAY, gtm.step_size(t1, t4) )
    assert_equals( 59*DAY, gtm.step_size(t2, t4) )

  def test_ttl(self):
    DAY = 60*60*24
    gt = GregorianTime( 'daily' )
    assert_equals( 3*DAY, gt.ttl( 3 ) )
    assert_equals( 3*DAY, gt.ttl( 3, relative_time=time.time() ) )
    assert_equals( 4*DAY, gt.ttl( 3, relative_time=time.time()+DAY ) )
    assert_equals( 8*DAY, gt.ttl( 3, relative_time=time.time()+(5*DAY) ) )
    assert_equals( 2*DAY, gt.ttl( 3, relative_time=time.time()-DAY ) )
    assert_equals( DAY, gt.ttl( 3, relative_time=time.time()-2*DAY ) )
    assert_equals( 0, gt.ttl( 3, relative_time=time.time()-3*DAY ) )
