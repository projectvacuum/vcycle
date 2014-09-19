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
import string
import tempfile
import ConfigParser

tenancies    = None
lastFizzles = {}

def readConf(requirePassword=True):

  global tenancies, lastFizzles
  
  tenancies = {}

  tenancyStrOptions = [ 'tenancy_name', 'url', 'username' ]

  tenancyIntOptions = [ 'max_machines' ]

  vmtypeStrOptions = [ 'ce_name', 'image_name', 'flavor_name', 'root_key_name', 'x509dn' ]

  vmtypeIntOptions = [ 'max_machines', 'backoff_seconds', 'fizzle_seconds', 'max_wallclock_seconds' ]

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

  # First look for tenancy sections

  for tenancySectionName in parser.sections():
    split1 = tenancySectionName.lower().split(None,1)

    if split1[0] == 'vmtype':    
      continue
      
    elif split1[0] != 'tenancy':
      return 'Section type ' + split1[0] + ' not recognised'
      
    else:
      tenancyName = split1[1]
      
      if string.translate(tenancyName, None, '0123456789abcdefghijklmnopqrstuvwyz-_') != '':
        return 'Name of tenancy section [tenancy ' + tenancyName + '] can only contain a-z 0-9 - or _'
      
      tenancy = {}
      
      # Get the options from this section for this tenancy
        
      for opt in tenancyStrOptions:
        if parser.has_option(tenancySectionName, opt):
          tenancy[opt] = parser.get(tenancySectionName, opt)
        else:
          return 'Option ' + opt + ' required in [' + tenancySectionName + ']'

      for opt in tenancyIntOptions:
        try:
          tenancy[opt] = int(parser.get(tenancySectionName, opt))
        except:
          return 'Option ' + opt + ' required in [' + tenancySectionName + ']'

      try:
        # We use ROT-1 (A -> B etc) encoding so browsing around casually doesn't
        # reveal passwords in a memorable way. 
        tenancy['password'] = ''.join([ chr(ord(c)-1) for c in parser.get(tenancySectionName, 'password')])
      except:
        if requirePassword:
          return 'Option password is required in [' + tenancySectionName + ']'
        else:
          tenancy['password'] = ''

      try:
        tenancy['delete_old_files'] = bool(parser.get(tenancySectionName, 'delete_old_files'))
      except:
        tenancy['delete_old_files'] = True

      # Get the options for each vmtype section associated with this tenancy

      vmtypes = {}

      for vmtypeSectionName in parser.sections():
        split2 = vmtypeSectionName.lower().split(None,2)

        if split2[0] == 'vmtype':

          if split2[1] == tenancyName:
            vmtypeName = split2[2]

            if string.translate(vmtypeName, None, '0123456789abcdefghijklmnopqrstuvwyz-_') != '':
              return 'Name of vmtype section [vmtype ' + tenancyName + ' ' + vmtypeName + '] can only contain a-z 0-9 - or _'
      
            vmtype = {}

            for opt in vmtypeStrOptions:              
              if parser.has_option(vmtypeSectionName, opt):
                vmtype[opt] = parser.get(vmtypeSectionName, opt)
              else:
                return 'Option ' + opt + ' required in [' + vmtypeSectionName + ']'

            for opt in vmtypeIntOptions:
              try:
                vmtype[opt] = int(parser.get(vmtypeSectionName, opt))
              except:
                return 'Option ' + opt + ' required in [' + vmtypeSectionName + ']'

            try:
              vmtype['heartbeat_file'] = parser.get(vmtypeSectionName, 'heartbeat_file')
            except:
              pass

            try:
              vmtype['heartbeat_seconds'] = int(parser.get(vmtypeSectionName, 'heartbeat_seconds'))
            except:
              pass

            try:
              vmtype['target_share'] = float(parser.get(vmtypeSectionName, 'target_share'))
            except:
              return 'Option target_share required in [' + vmtypeSectionName + ']'
            
            if tenancyName not in lastFizzles:
              lastFizzles[tenancyName] = {}
              
            if vmtypeName not in lastFizzles[tenancyName]:
              lastFizzles[tenancyName][vmtypeName] = int(time.time()) - vmtype['backoff_seconds']

            vmtypes[vmtypeName] = vmtype

      if len(vmtypes) < 1:
        return 'No vmtypes defined for tenancy ' + tenancyName + ' - each tenancy must have at least one vmtype'

      tenancy['vmtypes']     = vmtypes
      tenancies[tenancyName] = tenancy

  return None

def createFile(targetname, contents, mode=None):
  # Create a text file containing contents in the vcycle tmp directory
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
