import os
import time
import ConfigParser
from mock import patch

from vcycle.core import shared
from vcycle.core import vacutils
from vcycle.test.test_api import TestSpace
from vcycle.test.test_api import CycleTime

ct = CycleTime()

@patch('vcycle.core.shared.file_driver', autospec = True)
@patch('vcycle.core.vacutils.createUserData', autospec = True)
@patch('time.time', side_effect = ct.time)
def test(_0, _1, _2):
  # Set up parser
  parser = ConfigParser.RawConfigParser()
  conf_path = (os.path.abspath(os.path.dirname(__file__))
      + '/test_configs/test.conf')
  parser.read(conf_path)

  # readConf with test parser
  shared.readConf(parser = parser)

  for _ in range(20):
    print "\ncycle: ", time.time(), "\n"
    for space in shared.getSpaces().values():
      space.oneCycle()
      ct.update()

  # look at machines in the space
  print map(lambda x: x.state, space.machines.values())

test()
