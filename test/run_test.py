#! /usr/bin/python2

import os
import sys
import time
import ConfigParser
from mock import patch

from vcycle.test.test_api import CycleTime
from vcycle.test.test_api import TestManager

ct = CycleTime()

@patch('vcycle.core.shared.file_driver', autospec = True)
@patch('vcycle.core.vacutils.createUserData', autospec = True)
@patch('time.time', side_effect = ct.time)
@patch('vcycle.core.vacutils.logLine', autospec = True)
def test(file_name, cycles, _0, _1, _2, _3):
  """ Test function that wraps around the TestManager

  Allows some features to be mocked out for everything contained within
  using cycles instead of seconds, mocking out functions dealing with
  files, and the logLine function.
  """

  tm = TestManager(file_name, cycles)
  tm.run()

if __name__ == '__main__':
  test(sys.argv[1], int(sys.argv[2]))
