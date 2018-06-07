import os
import time
import ConfigParser
from mock import patch

from vcycle.test.test_api import CycleTime
from vcycle.test.test_api import TestManager

ct = CycleTime()

@patch('vcycle.core.shared.file_driver', autospec = True)
@patch('vcycle.core.vacutils.createUserData', autospec = True)
@patch('time.time', side_effect = ct.time)
def test(_0, _1, _2):

  tm = TestManager('test.conf')

  for _ in range(100):
    tm.cycle()

  # look at machines in the space
  for space in tm.spaces.values():
    print space
    print map(lambda x: x.state, space.machines.values())
    print map(lambda x: x.job, space.machines.values())
    print map(lambda x: x.jobID, space.machines.values())

test()
