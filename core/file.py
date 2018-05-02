import os
import stat
import shutil

from vcycle.core import vacutils


class File(object):
  """ Object that encapsulates file interactions """

  def __init__(self, base_dir = '/var/lib/vcycle',
               tmp_dir = '/var/lib/vcycle/tmp'):
    self.base_dir = base_dir
    self.tmp_dir = tmp_dir

  def create_dir(self, dir_name, mode):
    os.makedirs(self.base_dir + '/' +  dir_name, mode)

  def create_file(
      self, file_name, content,
      mode = stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP):
    vacutils.createFile(self.base_dir + '/' + file_name, content, mode)

  def remove_dir(self, dir_name):
    full_dir = self.base_dir + '/' + dir_name
    vacutils.logLine('Found and deleted ' + full_dir)
    shutil.rmtree(full_dir)
