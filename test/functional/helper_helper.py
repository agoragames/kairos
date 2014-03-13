import os
import time
import unittest
from datetime import *

from chai import Chai

from kairos.timeseries import *

# mongo expiry requires absolute time vs. redis ttls, so adjust it in whole hours
def _time(t):
  return (500000*3600)+t

TEST_ALL = os.environ.get('TEST_ALL','true')
