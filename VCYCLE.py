#!/usr/bin/python
#
#  VCYCLE.py - vcycle library
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-4. All rights reserved.
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

import os
import sys
import time
import json
import tempfile
import ConfigParser

vmtypes     = None
lastFizzles = {}

def readConf(requirePassword=True):

  global vmtypes, lastFizzles
  
  vmtypes = {}

  vmtypeStrOptions = ( 'project_name', 'auth_url', 'username', 'space_name', 'image_name', 
                       'flavor_name', 'root_key_name', 'x509dn' )

  vmtypeIntOptions = ( 'servers_total', 'backoff_seconds', 'fizzle_seconds', 'max_wallclock_seconds' ) 

  parser = ConfigParser.RawConfigParser()
  
  # Look for configuration files in /etc/vcycle.d
  try:
    confFiles = os.listdir('/etc/vcycle.d')
  except:
    pass 
  else:
    for oneFile in sorted(confFiles):
      if oneFile[-5:] == '.conf':      
        try:
          parser.read('/etc/vcycle.d/' + oneFile)
        except Exception as e:
          logLine('Failed to parse /etc/vcycle.d/' + oneFile + ' (' + str(e) + ')')

  # Standalone configuration file, read last in case of manual overrides
  parser.read('/etc/vcycle.conf')

#  for sectionName in parser.sections():
  if length(parser.sections()) == 1):
    sectionName = parser.sections()[0]
    
    sectionNameSplit = sectionName.lower().split(None,1)

    if sectionNameSplit[0] == 'vmtype':
      vmtype = {}

      for opt in vmtypeStrOptions:
        if parser.has_option(sectionName, opt):
          vmtype[opt] = parser.get(sectionName, opt)
        else:
          return 'Option ' + opt + ' required in [vmtype ' + sectionNameSplit[1] + ']'

      for opt in vmtypeIntOptions:
        try:
          vmtype[opt] = int(parser.get(sectionName, opt))
        except:
          return 'Option ' + opt + ' required in [vmtype ' + sectionNameSplit[1] + ']'

      try:
        vmtype['password'] = parser.get(sectionName, 'password')
      except:
        if requirePassword:
          return 'Option password is required in [vmtype ' + sectionNameSplit[1] + ']'
        else:
          vmtype['password'] = ''

      try:
        vmtype['heartbeat_file'] = parser.get(sectionName, 'heartbeat_file')
      except:
        pass

      try:
        vmtype['heartbeat_seconds'] = int(parser.get(sectionName, 'heartbeat_seconds'))
      except:
        pass

      if not sectionNameSplit[1] in lastFizzles:
        lastFizzles[sectionNameSplit[1]] = int(time.time()) - vmtype['backoff_seconds']

      vmtypes[sectionNameSplit[1]] = vmtype 

    else:
      return 'Section type ' + sectionNameSplit[0] + ' not recognised'

  else:
    return 'Can not define more than one vmtype in configuration yet!'
      
  return None

def createFile(targetname, contents, mode=None):
  # Create a text file containing contents in the vac tmp directory
  # then move it into place. Rename is an atomic operation in POSIX,
  # including situations where targetname already exists.
   
  try:
    ftup = tempfile.mkstemp(prefix='/var/lib/vcycle/tmp/temp',text=True)
    os.write(ftup[0], contents)
       
    if mode: 
      os.fchmod(ftup[0], mode)

    os.close(ftup[0])
    os.rename(ftup[1], targetname)
    return True
  except:
    return False

def makeJsonFile(targetDirectory):
  # Create a dictionary containing the keys and values from the given directory
  # and then write to .json in that directory
  
  outputDict = {}
  
  try:
    filesList = os.listdir(targetDirectory)
  except Exception as e:
         logLine('Listing directory ' + targetDirectory + ' fails with ' + str(e))
  else:
    for oneFile in filesList:
     if oneFile[0] != '.' and not os.path.isdir(targetDirectory + '/' + oneFile):
       try:
         outputDict[oneFile] = open(targetDirectory + '/' + oneFile).read()
       except Exception as e:
         logLine('Failed reading ' + targetDirectory + '/' + oneFile + ' with ' + str(e))
         pass

  try:
    f = open(targetDirectory + '/.json', 'w')
    json.dump(outputDict, f)
    f.close()   
  except Exception as e:
    logLine('Writing JSON fails with ' + str(e))

def logLine(text):
  sys.stderr.write(time.strftime('%b %d %H:%M:%S [') + str(os.getpid()) + ']: ' + text + '\n')
  sys.stderr.flush()
