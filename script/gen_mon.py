#!/usr/bin/env python

from kairos import Timeseries
import redis
import time
import datetime
import random

MAX_WIDTH_COLUMNS = 60

KEY = 'example.com'
KEY_PREFIX = 'timedata:domain'

client = redis.Redis('localhost', 6379)
counters = Timeseries(client, {
        'minute': {
                'step': 60,              # 60 seconds
                'steps': 60,             # last hour
                'count_only' : True,    # store counts only.
            },
        'hour': {
                'step': 3600,           # Hourly
                'steps': 24,            # Last day
                'count_only' : True,    # Store counts only.
            },
        'daily': {
                'step': 86400,          # Daily
                'steps': 30,            # Last 30 days
                'count_only': True,     # Store counts only.
            },
    }, 
    key_prefix=KEY_PREFIX)

def hit(domain):
    print "hit: %s @ %d" % (domain, time.time())
    counters.insert(domain, 1)

def dump_series(base_time, series):
    for ts, value in series.iteritems():
        print "%02d(%02d)" % ((ts-base_time)/60, value), 
    print
    
def plot_series(base_time, series, max_val):
    scale = max_val / MAX_WIDTH_COLUMNS
    for ts, count in series.iteritems():
        print "%4d minutes (%03d): %s" % ((ts-base_time)/60, count, "*" * (count/scale))

def sum_series(series):
    # series to list: series.list()
    return sum(series.values())

def generate():
    # record a couple of hits.
    hit(KEY)
    hit(KEY)
    
    start = datetime.datetime.now()
    x = 0
    while True:
        time.sleep(1)
        # Record a hit every once in a while (approx. every 3.5 seconds...)
        if x % random.randint(2,5) == 0:
            hit(KEY)
        x += 1

interval_max_values = { 'minute' : 100, 'hour': 2000, 'daily': 2000*24 }

def monitor(interval_name):
    while True:
        # get = counters.get(KEY, interval_name)
        series = counters.series(KEY, interval_name)
        # count = counters.count(KEY, interval_name)
        last_5 = counters.series(KEY, interval_name, steps=5, condensed=False)
        sum = sum_series(last_5)
        # This should work but breaks: sum = counters.series(KEY, interval_name, steps=5, condensed=True)
        #dump_series(time.time(), series)
        plot_series(time.time(), series, interval_max_values[interval_name])
        print "%d in last 5 %s (~%2.2f per %s)." % (sum, interval_name, sum/5.0, interval_name)
        time.sleep(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Test something.')
    parser.add_argument('op', metavar='op', default='generate', action="store", choices=['generate', 'monitor'])
    parser.add_argument('-i', '--interval', metavar='interval', default='minute', action='store', choices=['minute', 'hour', 'daily'])
    opts = parser.parse_args()
    if opts.op == 'generate':
        generate()
    else:
        monitor(opts.interval)
