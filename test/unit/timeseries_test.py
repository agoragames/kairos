'''
Functional tests for timeseries core
'''
import time
import datetime

from kairos.timeseries import *
from chai import Chai

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
