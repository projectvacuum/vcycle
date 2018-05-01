import os
import ConfigParser
from mock import patch

from vcycle.core import shared
from vcycle.test.test_api import TestSpace


@patch('vcycle.core.shared.file_driver', autospec = True)
@patch('vcycle.core.vacutils.createUserData', autospec = True)
def test(mockCreateFile, mockCreateUserData):
  # Set up parser
  parser = ConfigParser.RawConfigParser()
  conf_path = (os.path.abspath(os.path.dirname(__file__))
      + '/test_configs/test.conf')
  parser.read(conf_path)

  # readConf with test parser
  shared.readConf(parser = parser)
  for _, space in shared.getSpaces().iteritems():
    try:
      space.oneCycle()
      print space.machines
    except:
      print 'failed!'

  print shared.getSpaces()


test()
