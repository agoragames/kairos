'''
Functional tests for timeseries core
'''
import time
import datetime

from kairos.timeseries import *
from chai import Chai

class UrlTest(Chai):
  def test_bad_url(self):
    with assert_raises(ImportError):
      Timeseries("noop://foo/bar")

class RelativeTimeTest(Chai):

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

  def test_ttl(self):
    DAY = 60*60*24
    rt = GregorianTime( 'daily' )
    assert_equals( 3*DAY, rt.ttl( 3 ) )
    assert_equals( 3*DAY, rt.ttl( 3, relative_time=time.time() ) )
    assert_equals( 4*DAY, rt.ttl( 3, relative_time=time.time()+DAY ) )
    assert_equals( 8*DAY, rt.ttl( 3, relative_time=time.time()+(5*DAY) ) )
    assert_equals( 2*DAY, rt.ttl( 3, relative_time=time.time()-DAY ) )
    assert_equals( DAY, rt.ttl( 3, relative_time=time.time()-2*DAY ) )
    assert_equals( 0, rt.ttl( 3, relative_time=time.time()-3*DAY ) )
