#
#  __init__.py for vcycle package
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2014-5. All rights reserved.
#
#  Redistribution and use in source and binary forms, with or
#  without modification, are permitted provided that the following
#  conditions are met:
#
#    o Redistributions of source code must retain the above
#      copyright notice, this list of conditions and the following
#      disclaimer.
#    o Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
#  CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
#  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
#  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
#  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
#  TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#  ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.
#
#  Contacts: Andrew.McNab@cern.ch  http://www.gridpp.ac.uk/vcycle/
#

from vcycle.shared   import *
from vcycle.vacutils import *

import os

# We import all modules of the form xxxx_api.py in the package directory
# and in first level folders
#
# The API object in the module is created by shared.py at runtime using
# the BaseSpace.__subclasses__() method.

vcycledir = os.path.dirname(__file__)

for dirname, dirnames, files in os.walk(vcycledir):
  # ignore .git folder and build folder
  if '.git' in dirnames:
    dirnames.remove('.git')
  if 'RPMTMP' in dirnames:
    dirnames.remove('RPMTMP')
  # look for api files and import them
  for apifile in files:
    if apifile.endswith('_api.py'):
      reldir = os.path.relpath(dirname, vcycledir)
      if reldir == '.':
        __import__('vcycle.' + apifile[:-3])
      else:
        __import__('vcycle.' + reldir + '.' + apifile[:-3])

del apifile, dirname, dirnames, files

__all__ = [ 'shared', 'vacutils' ]
