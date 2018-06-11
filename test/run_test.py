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
@patch('vcycle.core.vacutils.logLine', autospec = True)
def test(_0, _1, _2, _3):

  tm = TestManager('test.conf', 1000)

  tm.run()

test()
