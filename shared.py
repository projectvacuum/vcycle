#!/usr/bin/python
#
#  shared.py - common functions, classes, and variables for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-5. All rights reserved.
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

import pprint

import os
import re
import sys
import stat
import time
import json
import shutil
import string
import pycurl
import random
import base64
import StringIO
import tempfile
import calendar
import ConfigParser

import vcycle.vacutils

class VcycleError(Exception):
  pass

vcycleVersion       = None
spaces              = None
maxWallclockSeconds = 0

class MachineState:
  #
  # not listed -> starting
  # starting   -> failed or running or shutdown (if we miss the time when running)
  # running    -> shutdown
  # shutdown   -> deleting
  # deleting   -> not listed or failed
  #
  # random OpenStack unreliability can require transition to failed at any time
  # stopped file created when machine first seen in shutdown, deleting, or failed state
  #
  unknown, shutdown, starting, running, deleting, failed = ('Unknown', 'Shut down', 'Starting', 'Running', 'Deleting', 'Failed')
   
class Machine:

  def __init__(self, name, spaceName, state, ip, createdTime, startedTime, updatedTime, uuidStr):

    # Store values from api-specific calling function
    self.name         = name
    self.spaceName    = spaceName
    self.state        = state
    self.ip           = ip
    self.createdTime  = createdTime
    self.startedTime  = startedTime
    self.updatedTime  = updatedTime
    self.uuidStr      = uuidStr

    # Record when the machine started (rather than just being created)
    if startedTime and not os.path.isfile('/var/lib/vcycle/machines/' + name + '/started'):
      vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + name + '/started', str(startedTime), 0600, '/var/lib/vcycle/tmp')

    # Store value stored when we requested the machine
    try:
      f = open('/var/lib/vcycle/machines/' + name + '/vmtype_name', 'r')
    except:
      self.vmtypeName = None
    else:
      self.vmtypeName = f.read().strip()
      f.close()

    try:
      spaces[self.spaceName].totalMachines += 1
      spaces[self.spaceName].vmtypes[self.vmtypeName].totalMachines += 1

      if spaces[self.spaceName].vmtypes[self.vmtypeName].target_share > 0.0:
        spaces[self.spaceName].vmtypes[self.vmtypeName].weightedMachines += (1.0 / spaces[self.spaceName].vmtypes[self.vmtypeName].target_share)
    except:
      pass
      
    if self.state == MachineState.running:
      try:
        spaces[self.spaceName].runningMachines += 1
        spaces[self.spaceName].vmtypes[self.vmtypeName].runningMachines += 1
      except:
        pass

    try:        
      if self.state == MachineState.starting or \
         (self.state == MachineState.running and \
          ((int(time.time()) - startedTime) < spaces[self.spaceName].vmtypes[self.vmtypeName].fizzle_seconds)):
        spaces[self.spaceName].vmtypes[self.vmtypeName].notPassedFizzle += 1
    except:      
      pass

    # Possibly created by the machine itself
    try:
      self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + name + '/machineoutputs/vm-heartbeat').st_ctime)
    except:
      self.heartbeatTime = None

    # Check if the machine already has a stopped timestamp
    try:
      self.stoppedTime = int(open('/var/lib/vcycle/machines/' + name + '/stopped', 'r').read())
    except:
      if self.state == MachineState.shutdown or self.state == MachineState.failed or self.state == MachineState.deleting:
        # Record that we have seen the machine in a stopped state for the first time
        # updateTime has the last transition time, presumably to being stopped.
        # This is certainly a better estimate than using time.time()?
        self.stoppedTime = updatedTime
        vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + name + '/stopped', str(self.stoppedTime), 0600, '/var/lib/vcycle/tmp')

        # Record the shutdown message if available
        try:
          self.shutdownMessage = open('/var/lib/vcycle/machines/' + name + '/machineoutputs/shutdown_message', 'r').read().strip()
          vcycle.vacutils.logLine('Machine ' + name + ' shuts down with message "' + self.shutdownMessage + '"')
          shutdownCode = int(self.shutdownMessage.split(' ')[0])
        except:
          self.shutdownMessage = None
          shutdownCode = None

        if self.vmtypeName:
          # Store last abort time for stopped machines, based on shutdown message code
          if shutdownCode and \
             (shutdownCode >= 300) and \
             (shutdownCode <= 699) and \
             (self.stoppedTime > spaces[self.spaceName].vmtypes[self.vmtypeName].lastAbortTime):
            vcycle.vacutils.logLine('Set ' + self.spaceName + ' ' + self.vmtypeName + ' lastAbortTime ' + str(self.stoppedTime) + 
                                    ' due to ' + name + ' shutdown message')
            spaces[self.spaceName].vmtypes[self.vmtypeName].setLastAbortTime(self.stoppedTime)
              
          elif (self.stoppedTime > spaces[self.spaceName].vmtypes[self.vmtypeName].lastAbortTime) and \
               ((self.stoppedTime - self.startedTime) < spaces[self.spaceName].vmtypes[self.vmtypeName].fizzle_seconds): 

            # Store last abort time for stopped machines, based on fizzle_seconds
            vcycle.vacutils.logLine('Set ' + self.spaceName + ' ' + self.vmtypeName + ' lastAbortTime ' + str(self.stoppedTime) +
                                    ' due to ' + name + ' fizzle')
            spaces[self.spaceName].vmtypes[self.vmtypeName].setLastAbortTime(self.stoppedTime)

          if shutdownCode and (shutdownCode / 100) == 3:
            vcycle.vacutils.logLine('For ' + self.spaceName + ':' + self.vmtypeName + ' minimum fizzle_seconds=' +
                                      str(self.stoppedTime - self.startedTime) + ' ?')
        
        self.writeApel()
      else:
        self.stoppedTime = None

    vcycle.vacutils.logLine('= ' + name + ' in ' + 
                            str(self.spaceName) + ':' +
                            str(self.vmtypeName) + ' ' + 
                            self.ip + ' ' + 
                            self.state + ' ' + 
                            str(self.createdTime) + '-' +
                            str(self.startedTime) + '-' +
                            str(self.updatedTime) + '-' +
                            str(self.stoppedTime) + ' ' +
                            str(self.heartbeatTime))
                            
  def writeApel(self):

    # If the VM just ran for fizzle_seconds, then we don't log it
    if (self.stoppedTime - self.startedTime) < spaces[self.spaceName].vmtypes[self.vmtypeName].fizzle_seconds:
      return
        
    nowTime = time.localtime()

    try:
      os.makedirs(time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
    except:
      pass

    try:
      os.makedirs(time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
    except:
      pass
      
    userDN = ''
    for component in self.spaceName.split('.'):
      userDN = '/DC=' + component + userDN

    if hasattr(spaces[self.spaceName].vmtypes[self.vmtypeName], 'accounting_fqan'):
      userFQANField = 'FQAN: ' + spaces[self.spaceName].vmtypes[self.vmtypeName].accounting_fqan + '\n'
    else:
      userFQANField = ''
      
    if hasattr(spaces[self.spaceName].vmtypes[self.vmtypeName], 'mb'):    
      memoryField = 'MemoryReal: ' + str(spaces[self.spaceName].vmtypes[self.vmtypeName].mb * 1024) + '\n' \
                    'MemoryVirtual: ' + str(spaces[self.spaceName].vmtypes[self.vmtypeName].mb * 1024) + '\n'
    else:
      memoryField = ''

    if hasattr(spaces[self.spaceName].vmtypes[self.vmtypeName], 'cpus'):    
      cpusField = 'Processors: ' + str(spaces[self.spaceName].vmtypes[self.vmtypeName].cpus) + '\n'
    else:
      cpuField = ''

    mesg = ('APEL-individual-job-message: v0.3\n' + 
            'Site: ' + spaces[self.spaceName].vmtypes[self.vmtypeName].gocdb_sitename + '\n' +
            'SubmitHost: ' + self.spaceName + '/vcycle-' + self.vmtypeName + '\n' +
            'LocalJobId: ' + self.uuidStr + '\n' +
            'LocalUserId: ' + self.name + '\n' +
            'Queue: ' + self.vmtypeName + '\n' +
            'GlobalUserName: ' + userDN + '\n' +
            userFQANField +
            'WallDuration: ' + str(self.stoppedTime - self.startedTime) + '\n' +
            # Can we do better for CpuDuration???
            'CpuDuration: ' + str(self.stoppedTime - self.startedTime) + '\n' +
            cpusField +
            'NodeCount: 1\n' +
            'InfrastructureDescription: APEL-VCYCLE\n' +
            'InfrastructureType: grid\n' +
            'StartTime: ' + str(self.startedTime) + '\n' +
            'EndTime: ' + str(self.stoppedTime) + '\n' +
            memoryField +
            'ServiceLevelType: HEPSPEC\n' +
            'ServiceLevel: ' + str(self.hs06) + '\n')

    fileName = time.strftime('%H%M%S', nowTime) + str(time.time() % 1)[2:][:8]
                          
    try:
      vcycle.vacutils.createFile(time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vcycle/tmp')
    except:
      vcycle.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d/', nowTime) + fileName)
      return

    try:
      vcycle.vacutils.createFile(time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vcycle/tmp')
    except:
      vcycle.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d/', nowTime) + fileName)
      return

class Vmtype:

  def __init__(self, spaceName, vmtypeName, parser, vmtypeSectionName):
  
    global maxWallclockSeconds
  
    self.spaceName  = spaceName
    self.vmtypeName = vmtypeName

    # Recreate lastAbortTime (must be set/updated with setLastAbortTime() to create file)
    try:
      f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName + '/last_abort_time', 'r')
    except:
      self.lastAbortTime = 0
    else:     
      self.lastAbortTime = int(f.read().strip())
      f.close()
  
    vcycle.vacutils.logLine('At ' + str(int(time.time())) + ' lastAbortTime for ' + spaceName + ':' + vmtypeName + ' set to ' + str(self.lastAbortTime))

    try:
      self.root_image = parser.get(vmtypeSectionName, 'root_image')
    except Exception as e:
      raise VcycleError('root_image is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.flavor_name = parser.get(vmtypeSectionName, 'flavor_name')
    except Exception as e:
      raise VcycleError('flavor_name is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.root_public_key = parser.get(vmtypeSectionName, 'root_public_key')
    except:
      self.root_public_key = None
    
    try:
      self.x509dn = parser.get(vmtypeSectionName, 'x509dn')
    except:
      self.x509dn = None
    
    try:
      if parser.has_option(vmtypeSectionName, 'max_machines'):
        self.max_machines = int(parser.get(vmtypeSectionName, 'max_machines'))
      else:
        self.max_machines = None
    except Exception as e:
      raise VcycleError('Failed to parse max_machines in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.backoff_seconds = int(parser.get(vmtypeSectionName, 'backoff_seconds'))
    except Exception as e:
      raise VcycleError('backoff_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.fizzle_seconds = int(parser.get(vmtypeSectionName, 'fizzle_seconds'))
    except Exception as e:
      raise VcycleError('fizzle_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      if parser.has_option(vmtypeSectionName, 'max_wallclock_seconds'):
        self.max_wallclock_seconds = int(parser.get(vmtypeSectionName, 'max_wallclock_seconds'))
      else:
        self.max_wallclock_seconds = 86400
      
      if self.max_wallclock_seconds > maxWallclockSeconds:
        maxWallclockSeconds = self.max_wallclock_seconds
    except Exception as e:
      raise VcycleError('max_wallclock_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.heartbeat_file = parser.get(vmtypeSectionName, 'heartbeat_file')
    except:
      self.heartbeat_file = None

    try:
      if parser.has_option(vmtypeSectionName, 'heartbeat_seconds'):
        self.heartbeat_seconds = int(parser.get(vmtypeSectionName, 'heartbeat_seconds'))
      else:
        self.heartbeat_seconds = None
    except Exception as e:
      raise VcycleError('Failed to parse heartbeat_seconds in [' + vmtypeSectionName + '] (' + str(e) + ')')

    if parser.has_option(vmtypeSectionName, 'accounting_fqan'):
      self.accounting_fqan = parser.get(vmtypeSectionName, 'accounting_fqan').strip()

    try:
      self.hs06 = parser.get(vmtypeSectionName, 'hs06')
    except:
      self.hs06 = 1.0
  
    try:
      self.user_data = parser.get(vmtypeSectionName, 'user_data')
    except Exception as e:
      raise VcycleError('user_data is required in [' + vmtypeSectionName + '] (' + str(e) + ')')

    try:
      if parser.has_option(vmtypeSectionName, 'target_share'):
        self.target_share = float(parser.get(vmtypeSectionName, 'target_share'))
      else:
        self.target_share = 0.0
    except Exception as e:
      raise VcycleError('Failed to parse target_share in [' + vmtypeSectionName + '] (' + str(e) + ')')

    if parser.has_option(vmtypeSectionName, 'log_machineoutputs') and \
               parser.get(vmtypeSectionName, 'log_machineoutputs').strip().lower() == 'true':
      self.log_machineoutputs = True
    else:
      self.log_machineoutputs = False

    try:
      if parser.has_option(vmtypeSectionName, 'machineoutputs_days'):
        self.machineoutputs_days = float(parser.get(vmtypeSectionName, 'machineoutputs_days'))
      else:
        self.machineoutputs_days = 3.0
    except Exception as e:
      raise VcycleError('Failed to parse machineoutputs_days in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    self.options = {}
    
    for (oneOption, oneValue) in parser.items(vmtypeSectionName):
      if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):
        if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
          raise VcycleError('Name of user_data_xxx (' + oneOption + ') must only contain a-z 0-9 and _')
        else:
          self.options[oneOption] = oneValue

    if parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                not parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      raise VcycleError('user_data_proxy_cert given but user_data_proxy_key missing (they can point to the same file if necessary)')
    elif not parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      raise VcycleError('user_data_proxy_key given but user_data_proxy_cert missing (they can point to the same file if necessary)')
    elif parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      self.user_data_proxy_cert = parser.get(vmtypeSectionName, 'user_data_proxy_cert')
      self.user_data_proxy_key  = parser.get(vmtypeSectionName, 'user_data_proxy_key')
             
    if parser.has_option(vmtypeSectionName, 'legacy_proxy') and \
       parser.get(vmtypeSectionName, 'legacy_proxy').strip().lower() == 'true':
      self.legacy_proxy = True
    else:
      self.legacy_proxy = False

    # Just for this instance, so Total for this vmtype in one space
    self.totalMachines    = 0
    self.runningMachines  = 0
    self.weightedMachines = 0.0
    self.notPassedFizzle  = 0

  def setLastAbortTime(self, abortTime):

    if abortTime > self.lastAbortTime:
      self.lastAbortTime = abortTime

      try:
        os.makedirs('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName,
                    stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
      except:
        pass
        
      vcycle.vacutils.createFile('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName + '/last_abort_time',
                                 str(abortTime), tmpDir = '/var/lib/vcycle/tmp')

  def makeMachineName(self):
    """Construct a machine name including the vmtype"""

    return 'vcycle-' + self.vmtypeName + '-' + ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
  
class BaseSpace(object):

  def __init__(self, api, spaceName, parser, spaceSectionName):
    self.api       = api
    self.spaceName = spaceName

    try:
      self.max_machines = int(parser.get(spaceSectionName, 'max_machines'))
    except Exception as e:
      raise VcycleError('max_machines is required in [space ' + spaceName + '] (' + str(e) + ')')

    self.vmtypes = {}

    for vmtypeSectionName in parser.sections():
      try:
        (sectionType, spaceTemp, vmtypeName) = vmtypeSectionName.lower().split(None,2)
      except:
        continue

      if sectionType != 'vmtype' or spaceTemp != spaceName:
        continue

      if string.translate(vmtypeName, None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
        raise VcycleError('Name of vmtype in [vmtype ' + spaceName + ' ' + vmtypeName + '] can only contain a-z 0-9 or -')

      try:
        self.vmtypes[vmtypeName] = Vmtype(spaceName, vmtypeName, parser, vmtypeSectionName)
      except Exception as e:
        raise VcycleError('Failed to initialize [vmtype ' + spaceName + ' ' + vmtypeName + '] (' + str(e) + ')')

    if len(self.vmtypes) < 1:
      raise VcycleError('No vmtypes defined for space ' + spaceName + ' - each space must have at least one vmtype!')

    # Start new curl session for this instance
    self.curl = pycurl.Curl()
    
    self.token = None

    # totalMachines includes ones Vcycle doesn't manage
    self.totalMachines   = 0
    self.runningMachines = 0
    
    # all the Vcycle-created VMs in this space
    self.machines = {}

  def httpRequest(self, url, request = None, headers = None, verbose = False, method = None, anyStatus = False):

    self.curl.unsetopt(pycurl.CUSTOMREQUEST)
    self.curl.setopt(pycurl.URL, str(url))
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycleVersion)
    
    if method and method.upper() == 'DELETE':
      self.curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
    elif not request:
      self.curl.setopt(pycurl.HTTPGET, True)
    else:
      try:
        self.curl.setopt(pycurl.POSTFIELDS, json.dumps(request))
      except Exception as e:
        raise VcycleError('JSON encoding of "' + str(request) + '" fails (' + str(e) + ')')

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)
    
    headersBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.HEADERFUNCTION, headersBuffer.write)

    # Set up the list of headers to send in the request    
    allHeaders = []
    
    if request:
      allHeaders.append('Content-Type: application/json')
      allHeaders.append('Accept: application/json')

    if headers:
      allHeaders.extend(headers)

    self.curl.setopt(pycurl.HTTPHEADER, allHeaders)

    if verbose:
      self.curl.setopt(pycurl.VERBOSE, 2)
    else:
      self.curl.setopt(pycurl.VERBOSE, 0)

    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        
    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    try:
      self.curl.perform()
    except Exception as e:
      raise VcycleError('Failed to read ' + url + ' (' + str(e) + ')')
    
    headersBuffer.seek(0)
    oneLine = headersBuffer.readline()
    outputHeaders = { 'status' : [ oneLine[9:].strip() ] }
    
    while True:
    
      try:
        oneLine = headersBuffer.readline()
      except:
        break
      
      if not oneLine.strip():
        break
      
      headerNameValue = oneLine.split(':',1)

      # outputHeaders is a dictionary of lowercased header names
      # but the values are always lists, with one or more values (if multiple headers with the same name)
      if headerNameValue[0].lower() not in outputHeaders:
        outputHeaders[ headerNameValue[0].lower() ] = []

      outputHeaders[ headerNameValue[0].lower() ].append( headerNameValue[1].strip() )

    # If not a 2xx code then raise an exception unless anyStatus option given
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      if anyStatus:
        return { 'headers' : outputHeaders, 'response' : None, 'status' : self.curl.getinfo(pycurl.RESPONSE_CODE) }
      else:
        raise VcycleError('Query of ' + url + ' returns HTTP code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    if method and method.upper() == 'DELETE':
      return { 'headers' : outputHeaders, 'response' : None, 'status' : self.curl.getinfo(pycurl.RESPONSE_CODE) }

    try:
      return { 'headers' : outputHeaders, 'response' : json.loads(outputBuffer.getvalue()), 'status' : self.curl.getinfo(pycurl.RESPONSE_CODE) }
    except:
      return { 'headers' : outputHeaders, 'response' : None, 'status' : self.curl.getinfo(pycurl.RESPONSE_CODE) }

  def deleteMachines(self):
    # Delete machines in this space. We do not update totals here: next cycle is good enough.
      
    for machineName,machine in self.machines.iteritems():
    
      # Delete machines as appropriate
      if machine.state == MachineState.shutdown:
        self.deleteOneMachine(machineName)
      elif machine.state == MachineState.failed:
        self.deleteOneMachine(machineName)
      elif machine.state == MachineState.running and \
           machine.vmtypeName in self.vmtypes and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.vmtypes[machine.vmtypeName].max_wallclock_seconds)):
        self.deleteOneMachine(machineName)
      elif machine.state == MachineState.running and \
           machine.vmtypeName in self.vmtypes and \
           self.vmtypes[machine.vmtypeName].heartbeat_file and \
           self.vmtypes[machine.vmtypeName].heartbeat_seconds and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.vmtypes[machine.vmtypeName].fizzle_seconds)) and \
           (
            (machine.heartbeatTime is None) or 
            (machine.heartbeatTime < (int(time.time()) - self.vmtypes[machine.vmtypeName].heartbeat_seconds))
           ):
        self.deleteOneMachine(machineName)
      #
      # Also delete machines with powerState != 1 for +15mins?
      #

  def makeMachines(self):

    vcycle.vacutils.logLine('Space ' + self.spaceName + 
                            ' has ' + str(self.runningMachines) + 
                            ' running vcycle VMs out of ' + str(self.totalMachines) +
                            ' found in any state for any vmtype or none')
  
    for vmtypeName,vmtype in self.vmtypes.iteritems():
      vcycle.vacutils.logLine('vmtype ' + vmtypeName + 
                              ' has ' + str(vmtype.runningMachines) + 
                              ' running vcycle VMs out of ' + str(vmtype.totalMachines) +
                              ' found in any state. ' + str(vmtype.notPassedFizzle) +
                              ' not passed fizzle_seconds(' + str(vmtype.fizzle_seconds) +
                              ').')
  
    creationsPerCycle  = int(0.9999999 + self.max_machines * 0.1)
    creationsThisCycle = 0

    # Keep making passes through the vmtypes until limits exhausted
    while True:
      if self.totalMachines >= self.max_machines:
        vcycle.vacutils.logLine('Reached limit (%d) on number of machines to create for space %s' % (self.max_machines, self.spaceName))
        return

      if creationsThisCycle >= creationsPerCycle:
        vcycle.vacutils.logLine('Already reached limit of %d machine creations this cycle' % creationsThisCycle )
        return
      
      # For each pass, vmtypes are visited in a random order
      vmtypeNames = self.vmtypes.keys()
      random.shuffle(vmtypeNames)

      # Will record the best vmtype to create
      bestVmtypeName = None

      for vmtypeName in vmtypeNames:
        if self.vmtypes[vmtypeName].target_share <= 0.0:
          continue

        if self.vmtypes[vmtypeName].totalMachines >= self.vmtypes[vmtypeName].max_machines:
          vcycle.vacutils.logLine('Reached limit (' + str(self.vmtypes[vmtypeName].totalMachines) + ') on number of machines to create for vmtype ' + vmtypeName)
          continue

        if int(time.time()) < (self.vmtypes[vmtypeName].lastAbortTime + self.vmtypes[vmtypeName].backoff_seconds):
          vcycle.vacutils.logLine('Free capacity found for %s ... but only %d seconds after last abort' 
                                  % (vmtypeName, int(time.time()) - self.vmtypes[vmtypeName].lastAbortTime) )
          continue

        if (int(time.time()) < (self.vmtypes[vmtypeName].lastAbortTime + 
                                self.vmtypes[vmtypeName].backoff_seconds + 
                                self.vmtypes[vmtypeName].fizzle_seconds)) and \
           (self.vmtypes[vmtypeName].notPassedFizzle > 0):
          vcycle.vacutils.logLine('Free capacity found for ' + 
                                  vmtypeName + 
                                  ' ... but still within fizzle_seconds+backoff_seconds(' + 
                                  str(int(self.vmtypes[vmtypeName].backoff_seconds + self.vmtypes[vmtypeName].fizzle_seconds)) + 
                                  ') of last abort (' + 
                                  str(int(time.time()) - self.vmtypes[vmtypeName].lastAbortTime) + 
                                  's ago) and ' + 
                                  str(self.vmtypes[vmtypeName].notPassedFizzle) + 
                                  ' running but not yet passed fizzle_seconds (' + 
                                  str(self.vmtypes[vmtypeName].fizzle_seconds) + ')')
          continue

        if (not bestVmtypeName) or (self.vmtypes[vmtypeName].weightedMachines < self.vmtypes[bestVmtypeName].weightedMachines):
          bestVmtypeName = vmtypeName
                 
      if bestVmtypeName:
        vcycle.vacutils.logLine('Free capacity found for ' + bestVmtypeName + ' within ' + self.spaceName + ' ... creating')

        # This tracks creation attempts, whether successful or not
        creationsThisCycle += 1

        try:
          self.createMachine(bestVmtypeName)
        except Exception as e:
          vcycle.vacutils.logLine('Failed creating machine with vmtype ' + bestVmtypeName + ' in ' + self.spaceName + ' (' + str(e) + ')')
        else:
          # Update totals for newly created machines
          self.totalMachines += 1
          self.vmtypes[bestVmtypeName].totalMachines    += 1
          self.vmtypes[bestVmtypeName].weightedMachines += (1.0 / self.vmtypes[bestVmtypeName].target_share)
          self.vmtypes[bestVmtypeName].notPassedFizzle  += 1

      else:
        vcycle.vacutils.logLine('No more free capacity and/or suitable vmtype found within ' + self.spaceName)
        return
      
  def createMachine(self, vmtypeName):
    """Generic machine creation"""
  
    try:
      machineName = self.vmtypes[vmtypeName].makeMachineName()
    except Exception as e:
      vcycle.vacutils.logLine('Failed construction new machine name (' + str(e) + ')')

    try:
      shutil.rmtree('/var/lib/vcycle/machines/' + machineName)
      vcycle.vacutils.logLine('Found and deleted left over /var/lib/vcycle/machines/' + machineName)
    except:
      pass

    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/machinefeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/jobfeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/machineoutputs',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + 
                stat.S_IWGRP + stat.S_IXGRP + stat.S_IRGRP + 
                stat.S_IWOTH + stat.S_IXOTH + stat.S_IROTH)

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/vmtype_name', vmtypeName,  0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/space_name',  self.spaceName,   0644, '/var/lib/vcycle/tmp')

    try:
      vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/phys_cores', str(self.vmtypes[vmtypeName].cpus), 0644, '/var/lib/vcycle/tmp')
    except:
      pass
      
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/hs06', str(self.vmtypes[vmtypeName].hs06), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/shutdown_time',
                               str(int(time.time()) + self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/cpu_limit_secs',  
                               str(self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/wall_limit_secs', 
                               str(self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')

    try:
      userDataContents = vcycle.vacutils.createUserData(shutdownTime   = int(time.time() +
                                                                             self.vmtypes[vmtypeName].max_wallclock_seconds),
                                                        vmtypesPath    = '/var/lib/vcycle/vmtypes/' + self.spaceName,
                                                        options        = self.vmtypes[vmtypeName].options,
                                                        versionString  = 'Vcycle ' + vcycleVersion,
                                                        spaceName      = self.spaceName,
                                                        vmtypeName     = vmtypeName,
                                                        userDataPath   = self.vmtypes[vmtypeName].user_data,
                                                        hostName       = machineName + '.' + self.spaceName,
                                                        uuidStr        = None)
    except Exception as e:
      raise VcycleError('Failed getting user_data file (' + str(e) + ')')

    try:
      open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'w').write(userDataContents)
    except:
      raise VcycleError('Failed to writing /var/lib/vcycle/machines/' + machineName + '/user_data')

    return machineName

  def oneCycle(self):
  
    try:
      self.connect()
    except Exception as e:
      vcycle.vacutils.logLine('Skipping ' + self.spaceName + ' this cycle: ' + str(e))
      return

    try:
      self.scanMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Giving up on ' + self.spaceName + ' this cycle: ' + str(e))
      return
      
    try:
      self.deleteMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Deleting old machines in ' + self.spaceName + ' fails: ' + str(e))
      # We carry on because this isn't fatal
      
    try:
      self.makeMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Making machines in ' + self.spaceName + ' fails: ' + str(e))
      
def readConf():

  global vcycleVersion, spaces

  try:
    f = open('/var/lib/vcycle/VERSION', 'r')
    vcycleVersion = f.readline().split('=',1)[1].strip()
    f.close()
  except:
    vcycleVersion = '0.0.0'
  
  spaces = {}

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
          vcycle.vacutils.logLine('Failed to parse /etc/vcycle.d/' + oneFile + ' (' + str(e) + ')')

  # Standalone configuration file, read last in case of manual overrides
  parser.read('/etc/vcycle.conf')

  # Find the space sections
  for spaceSectionName in parser.sections():
 
    try:
      (sectionType, spaceName) = spaceSectionName.lower().split(None,1)
    except Exception as e:
      raise VcycleError('Cannot parse section name [' + spaceSectionName + '] (' + str(e) + ')')
    
    if sectionType == 'space':
    
      if string.translate(spaceName, None, '0123456789abcdefghijklmnopqrstuvwxyz-.') != '':
        raise VcycleError('Name of space section [space ' + spaceName + '] can only contain a-z 0-9 - or .')
      
      try:
        api = parser.get(spaceSectionName, 'api').lower()
      except:
        raise VcycleError('api missing from [space ' + spaceName + ']')

      for subClass in BaseSpace.__subclasses__():
        if subClass.__name__ == api.capitalize() + 'Space':
          try:
            spaces[spaceName] = subClass(api, spaceName, parser, spaceSectionName)
          except Exception as e:
            raise VcycleError('Failed to initialise space ' + spaceName + ' (' + str(e) + ')')
          else:
            break
            
      if spaceName not in spaces:
        raise VcycleError(api + ' is not a supported API for managing spaces')

    elif sectionType != 'vmtype':
      raise VcycleError('Section type ' + sectionType + 'not recognised')

  # else: Skip over vmtype sections, which are parsed during the class initialization

def cleanupMachines():
  """ Go through /var/lib/vcycle/machines deleting/saved expired directory trees """
  
  try:
    dirslist = os.listdir('/var/lib/vcycle/machines/')
  except:
    return

  # Go through the per-machine directories
  for machineName in dirslist:

    # Get the space name
    try:
      spaceName = open('/var/lib/vcycle/machines/' + machineName + '/space_name', 'r').read().strip()
    except:
      spaceName = None
    else:
      if machineName in spaces[spaceName].machines:
        # We never delete/log directories for machines that are still listed
        continue
      else:
        # If in a current space, but not listed, then delete immediately
        expireTime = 0

    # Get the time beyond which this machine shouldn't be here
    try:
      expireTime = int(open('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/shutdown_time', 'r').read().strip())
    except:
      # if the shutdown_time is missing, then we construct it using the longest lived vmtype in current config
      expireTime = int(os.stat('/var/lib/vcycle/machines/' + machineName).st_ctime) + maxWallclockSeconds

    if int(time.time()) > expireTime + 3600:

      # Get the vmtype
      try:
        vmtypeName = open('/var/lib/vcycle/machines/' + machineName + '/vmtype_name', 'r').read().strip()
      except:
        vmtypeName = None

      # Log machineoutputs if a current space and vmtype and logging is enabled
      if spaceName and \
         vmtypeName and \
         spaceName in spaces and \
         vmtypeName in spaces[spaceName].vmtypes and \
         spaces[spaceName].vmtypes[vmtypeName].log_machineoutputs:
        vcycle.vacutils.logLine('Saving machineoutputs to /var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName)
        logMachineOutputs(spaceName, vmtypeName, machineName)

      # Always delete the working copies
      try:
        shutil.rmtree('/var/lib/vcycle/machines/' + machineName)
        vcycle.vacutils.logLine('Deleted /var/lib/vcycle/machines/' + machineName)
      except:
        vcycle.vacutils.logLine('Failed deleting /var/lib/vcycle/machines/' + machineName)

def logMachineOutputs(spaceName, vmtypeName, machineName):

  if os.path.exists('/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName):
    # Copy (presumably) already exists so don't need to do anything
    return
   
  try:
    os.makedirs('/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName,
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
  except:
    vcycle.vacutils.logLine('Failed creating /var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName)
    return

  try:
    # Get the list of files that the VM wrote in its /etc/machineoutputs
    outputs = os.listdir('/var/lib/vcycle/machines/' + machineName + '/machineoutputs')
  except:
    vcycle.vacutils.logLine('Failed reading /var/lib/vcycle/machines/' + machineName + '/machineoutputs')
    return
        
  if outputs:
    # Go through the files one by one, adding them to the machineoutputs directory
    for oneOutput in outputs:

      try:
        # first we try a hard link, which is efficient in time and space used
        os.link('/var/lib/vcycle/machines/' + machineName + '/machineoutputs/' + oneOutput,
                '/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName + '/' + oneOutput)
      except:
        try:
          # if linking failed (different filesystems?) then we try a copy
          shutil.copyfile('/var/lib/vcycle/machines/' + machineName + '/machineoutputs/' + oneOutput,
                            '/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName + '/' + oneOutput)
        except:
          vcycle.vacutils.logLine('Failed copying /var/lib/vcycle/machines/' + machineName + '/machineoutputs/' + oneOutput + 
                                  ' to /var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + machineName + '/' + oneOutput)

def cleanupMachineoutputs():
  """Go through /var/lib/vcycle/machineoutputs deleting expired directory trees whether they are current spaces/vmtypes or not"""

  try:
    spacesDirslist = os.listdir('/var/lib/vcycle/machineoutputs/')
  except:
    return
      
  # Go through the per-machine directories
  for spaceDir in spacesDirslist:
  
    try:
      vmtypesDirslist = os.listdir('/var/lib/vcycle/machineoutputs/' + spaceDir)
    except:
      continue

    for vmtypeDir in vmtypesDirslist:
        
      try:
        hostNamesDirslist = os.listdir('/var/lib/vcycle/machineoutputs/' + spaceDir + '/' + vmtypeDir)
      except:
        continue
 
      for hostNameDir in hostNamesDirslist:

        # Expiration is based on file timestamp from when the COPY was created
        hostNameDirCtime = int(os.stat('/var/lib/vcycle/machineoutputs/' + spaceDir + '/' + vmtypeDir + '/' + hostNameDir).st_ctime)

        try: 
          expirationDays = spaces[spaceName].vmtypes[vmtypeDir].machineoutputs_days
        except:
          # use the default if something goes wrong (configuration file changed?)
          expirationDays = 3.0
           
        if hostNameDirCtime < (time.time() - (86400 * expirationDays)):
          try:
            shutil.rmtree('/var/lib/vcycle/machineoutputs/' + spaceDir + '/' + vmtypeDir + '/' + hostNameDir)
            vcycle.vacutils.logLine('Deleted /var/lib/vcycle/machineoutputs/' + spaceDir + '/' + vmtypeDir + 
                                    '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')
          except:
            vcycle.vacutils.logLine('Failed deleting /var/lib/vcycle/machineoutputs/' + spaceDir + '/' + 
                                    vmtypeDir + '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')

