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
      if self.state == starting or \
         (self.state == running and \
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
      self.stoppedTime = int(os.stat('/var/lib/vcycle/machines/' + name + '/stopped').st_ctime)
    except:
      if self.state == MachineState.shutdown or self.state == MachineState.failed or self.state == MachineState.deleting:
        # Record that we have seen the machine in a stopped state for the first time
        self.stoppedTime = int(time.time())
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

  def delete(self):
  
    vcycle.vacutils.logLine('Destroying ' + self.name + ' in ' + self.spaceName + ':' + str(self.vmtypeName) + ', in state ' + str(self.state))

    try:
      spaces[self.spaceName].httpJSON(spaces[self.spaceName].computeURL + '/servers/' + self.uuidStr,
                                      request = 'DELETE',
                                      headers = [ 'X-Auth-Token: ' + spaces[self.spaceName].token ])
    except Exception as e:
      raise VcycleError('Cannot delete ' + self.name + ' via ' + spaces[self.spaceName].computeURL + ' (' + str(e) + ')')

#    if self.spaceName and self.vmtypeName and spaces[self.spaceName].vmtypes[self.vmtypeName].log_machineoutputs:
#      vcycle.vacutils.logLine('Saving machineoutputs to /var/lib/vcycle/machineoutputs/' + self.spaceName + '/' + self.vmtypeName + '/' + self.name)
#      logMachineOutputs(self.spaceName, self.vmtypeName, self.name)
#
#    try:
#      shutil.rmtree('/var/lib/vcycle/machines/' + self.name)
#      vcycle.vacutils.logLine('Deleted /var/lib/vcycle/machines/' + self.name)
#    except:
#      vcycle.vacutils.logLine('Failed deleting /var/lib/vcycle/machines/' + self.name)

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
  
class BaseSpace:

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

  def httpJSON(self, url, request = None, headers = None, verbose = False):

    self.curl.unsetopt(pycurl.CUSTOMREQUEST)
    self.curl.setopt(pycurl.URL, str(url))
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycleVersion)
    
    if not request:
      self.curl.setopt(pycurl.HTTPGET, True)
    elif str(request).lower() == 'delete':
      self.curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
    else:
      try:
        self.curl.setopt(pycurl.POSTFIELDS, json.dumps(request))
      except Exception as e:
        raise VcycleError('JSON encoding of "' + str(request) + '" fails (' + str(e) + ')')

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)
    
    allHeaders = ['Content-Type: application/json', 'Accept: application/json']

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

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise VcycleError('Query of ' + url + ' returns HTTP code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    if str(request).lower() == 'delete':
      return None

    try:
      return json.loads(outputBuffer.getvalue())
    except Exception as e:
      if self.curl.getinfo(pycurl.RESPONSE_CODE) == 202 and \
         self.curl.getinfo(pycurl.REDIRECT_URL):
        return { 'location' : self.curl.getinfo(pycurl.REDIRECT_URL) }

      raise VcycleError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')

  def deleteMachines(self):
    # Delete machines in this space. We do not update totals here: next cycle is good enough.
      
    for machineName,machine in self.machines.iteritems():

#      # Store last abort time for stopped machines
#      if machine.vmtypeName and \
#         machine.vmtypeName in self.vmtypes and \
#         machine.stoppedTime and \
#         (machine.stoppedTime > self.vmtypes[machine.vmtypeName].lastAbortTime) and \
#         machine.startedTime and \
#         ((machine.stoppedTime - machine.startedTime) < self.vmtypes[machine.vmtypeName].fizzle_seconds): 
#        vcycle.vacutils.logLine('Set ' + self.spaceName + ' ' + machine.vmtypeName + ' lastAbortTime ' + str(machine.stoppedTime))
#        self.vmtypes[machine.vmtypeName].setLastAbortTime(machine.stoppedTime)
    
      # Delete machines as appropriate
      if machine.state == MachineState.shutdown:
        machine.delete()
      elif machine.state == MachineState.failed:
        machine.delete()
      elif machine.state == MachineState.running and \
           machine.vmtypeName in self.vmtypes and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.vmtypes[machine.vmtypeName].max_wallclock_seconds)):
        machine.delete()
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
        machine.delete()
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
                              ' found in any state')
  
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

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/phys_cores',    '1',        0644, '/var/lib/vcycle/tmp')
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
      
#
# The OpenstackSpace class is here in shared.py for now. 
# Will be split off into a separate per-API plugin file in the future.
#
class OpenstackSpace(BaseSpace):

  def __init__(self, api, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    BaseSpace.__init__(self, api, spaceName, parser, spaceSectionName)

    # OpenStack-specific initialization
    try:
      self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
    except Exception as e:
      raise VcycleError('tenancy_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.identityURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise VcycleError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.username = parser.get(spaceSectionName, 'username')
    except Exception as e:
      raise VcycleError('username is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      # We use ROT-1 (A -> B etc) encoding so browsing around casually doesn't
      # reveal passwords in a memorable way. 
      self.password = ''.join([ chr(ord(c)-1) for c in parser.get(spaceSectionName, 'password')])
    except Exception as e:
      raise VcycleError('password is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
  # Connect to the OpenStack service
  
    try:
      response = self.httpJSON(self.identityURL + '/tokens',
                               { 'auth' : { 'tenantName'           : self.tenancy_name, 
                                            'passwordCredentials' : { 'username' : self.username, 
                                                                      'password' : self.password 
                                                                    }
                                          }
                               } )
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.identityURL + ' (' + str(e) + ')')
 
    self.token     = str(response['access']['token']['id'])
    
    self.computeURL = None
    self.imageURL   = None
    
    for endpoint in response['access']['serviceCatalog']:
      if endpoint['type'] == 'compute':
        self.computeURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'image':
        self.imageURL = str(endpoint['endpoints'][0]['publicURL'])
        
    if not self.computeURL:
      raise VcycleError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise VcycleError('No image service URL found from ' + self.identityURL)

    vcycle.vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vcycle.vacutils.logLine('computeURL = ' + self.computeURL)
    vcycle.vacutils.logLine('imageURL   = ' + self.imageURL)

  def scanMachines(self):
    """Query OpenStack compute service for details of machines in this space"""
  
    try:
      response = self.httpJSON(self.computeURL + '/servers/detail',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneServer in response['servers']:

      # This includes VMs that we didn't create and won't manage, to avoid going above space limit
      self.totalMachines += 1

      # Just in case other VMs are in this space
      if oneServer['name'][:7] != 'vcycle-':
        continue

      uuidStr = str(oneServer['id'])

      try:
        ip = str(oneServer['addresses']['CERN_NETWORK'][0]['addr'])
      except:
        try:
          ip = str(oneServer['addresses']['novanetwork'][0]['addr'])
        except:
          ip = '0.0.0.0'

      createdTime  = calendar.timegm(time.strptime(str(oneServer['created']), "%Y-%m-%dT%H:%M:%SZ"))
      updatedTime  = calendar.timegm(time.strptime(str(oneServer['updated']), "%Y-%m-%dT%H:%M:%SZ"))

      try:
        startedTime = calendar.timegm(time.strptime(str(oneServer['OS-SRV-USG:launched_at']).split('.')[0], "%Y-%m-%dT%H:%M:%S"))
      except:
        startedTime = None

      taskState  = str(oneServer['OS-EXT-STS:task_state'])
      powerState = int(oneServer['OS-EXT-STS:power_state'])
      status     = str(oneServer['status'])
      
      if taskState == 'Deleting':
        state = MachineState.deleting
      elif status == 'ACTIVE' and powerState == 1:
        state = MachineState.running
      elif status == 'BUILD' or status == 'ACTIVE':
        state = MachineState.starting
      elif status == 'SHUTOFF':
        state = MachineState.shutdown
      elif status == 'ERROR':
        state = MachineState.failed
      elif status == 'DELETED':
        state = MachineState.deleting
      else:
        state = MachineState.unknown

      self.machines[oneServer['name']] = vcycle.shared.Machine(name        = oneServer['name'],
                                                               spaceName   = self.spaceName,
                                                               state       = state,
                                                               ip          = ip,
                                                               createdTime = createdTime,
                                                               startedTime = startedTime,
                                                               updatedTime = updatedTime,
                                                               uuidStr     = uuidStr)

  def getFlavorID(self, vmtypeName):
    """Get the "flavor" ID (## We're all living in Amerika! ##)"""
  
    if hasattr(self.vmtypes[vmtypeName], '_flavorID'):
      if self.vmtypes[vmtypeName]._flavorID:
        return self.vmtypes[vmtypeName]._flavorID
      else:
        raise VcycleError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')
      
    try:
      response = self.httpJSON(self.computeURL + '/flavors',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
    
    try:
      for flavor in response['flavors']:
        if flavor['name'] == self.vmtypes[vmtypeName].flavor_name:
          self.vmtypes[vmtypeName]._flavorID = str(flavor['id'])
          return self.vmtypes[vmtypeName]._flavorID
    except:
      pass
        
    raise VcycleError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')

  def getImageID(self, vmtypeName):
    """Get the image ID"""

    # If we already know the image ID, then just return it
    if hasattr(self.vmtypes[vmtypeName], '_imageID'):
      if self.vmtypes[vmtypeName]._imageID:
        return self.vmtypes[vmtypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise VcycleError('Image "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' not available!')

    # Get the existing images for this tenancy
    try:
      response = self.httpJSON(self.computeURL + '/images/detail',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    # Specific image, not managed by Vcycle, lookup ID
    if self.vmtypes[vmtypeName].root_image[:6] == 'image:':
      for image in response['images']:
         if self.vmtypes[vmtypeName].root_image[6:] == image['name']:
           self.vmtypes[vmtypeName]._imageID = str(image['id'])
           return self.vmtypes[vmtypeName]._imageID

      raise VcycleError('Image "' + self.vmtypes[vmtypeName].root_image[6:] + '" for vmtype ' + vmtypeName + ' not available!')

    # Always store/make the image name
    if self.vmtypes[vmtypeName].root_image[:7] == 'http://' or \
       self.vmtypes[vmtypeName].root_image[:8] == 'https://' or \
       self.vmtypes[vmtypeName].root_image[0] == '/':
      imageName = self.vmtypes[vmtypeName].root_image
    else:
      imageName = '/var/lib/vcycle/' + self.spaceName + '/' + vmtypeName + '/' + self.vmtypes[vmtypeName].root_image

    # Find the local copy of the image file
    if not hasattr(self.vmtypes[vmtypeName], '_imageFile'):

      if self.vmtypes[vmtypeName].root_image[:7] == 'http://' or \
         self.vmtypes[vmtypeName].root_image[:8] == 'https://':

        try:
          imageFile = vcycle.vacutils.getRemoteRootImage(self.vmtypes[vmtypeName].root_image,
                                         '/var/lib/vcycle/imagecache', 
                                         '/var/lib/vcycle/tmp')

          imageLastModified = int(os.stat(imageFile).st_mtime)
        except Exception as e:
          raise VcycleError('Failed fetching ' + self.vmtypes[vmtypeName].root_image + ' (' + str(e) + ')')

        self.vmtypes[vmtypeName]._imageFile = imageFile
 
      elif self.vmtypes[vmtypeName].root_image[0] == '/':
        
        try:
          imageLastModified = int(os.stat(self.vmtypes[vmtypeName].root_image).st_mtime)
        except Exception as e:
          raise VcycleError('Image file "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' does not exist!')

        self.vmtypes[vmtypeName]._imageFile = self.vmtypes[vmtypeName].root_image

      else: # root_image is not an absolute path, but imageName is
        
        try:
          imageLastModified = int(os.stat(imageName).st_mtime)
        except Exception as e:
          raise VcycleError('Image file "' + self.vmtypes[vmtypeName].root_image +
                            '" does not exist in /var/lib/vcycle/' + self.spaceName + '/' + vmtypeName + ' !')

        self.vmtypes[vmtypeName]._imageFile = imageName

    else:
      imageLastModified = int(os.stat(self.vmtypes[vmtypeName]._imageFile).st_mtime)

    # Go through the existing images looking for a name and time stamp match
# We should delete old copies of the current image name if we find them here
#    pprint.pprint(response)
    for image in response['images']:
      try:
         if image['name'] == imageName and \
            image['status'] == 'ACTIVE' and \
            image['metadata']['last_modified'] == str(imageLastModified):
           self.vmtypes[vmtypeName]._imageID = str(image['id'])
           return self.vmtypes[vmtypeName]._imageID
      except:
        pass

    vcycle.vacutils.logLine('Image "' + self.vmtypes[vmtypeName].root_image + '" not found in image service, so uploading')

    # Try to upload the image
    try:
      self.vmtypes[vmtypeName]._imageID = self.uploadImage(self.vmtypes[vmtypeName]._imageFile, imageName, imageLastModified)
      return self.vmtypes[vmtypeName]._imageID
    except Exception as e:
      raise VcycleError('Failed to upload image file ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageLastModified, verbose = False):

    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise VcycleError('Failed to open image file ' + imageName + ' (' + str(e) + ')')

    self.curl.setopt(pycurl.READFUNCTION,   f.read)
    self.curl.setopt(pycurl.UPLOAD,         True)
    self.curl.setopt(pycurl.CUSTOMREQUEST,  'POST')
    self.curl.setopt(pycurl.URL,            self.imageURL + '/v1/images')
    self.curl.setopt(pycurl.USERAGENT,      'Vcycle ' + vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT,        30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)

    self.curl.setopt(pycurl.HTTPHEADER,
                     [ 'x-image-meta-disk_format: raw', # <-- 'raw' for hdd; 'iso' for iso
                       'Content-Type: application/octet-stream',
                       'Accept: application/json',
                       'Transfer-Encoding: chunked',
                       'x-image-meta-container_format: bare',
                       'x-image-meta-is_public: False',                       
                       'x-image-meta-name: ' + imageName,
                       'x-image-meta-property-last-modified: ' + str(imageLastModified),
                       'X-Auth-Token: ' + self.token
                     ])

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)
    
    if verbose:
      self.curl.setopt(pycurl.VERBOSE, 2)
    else:
      self.curl.setopt(pycurl.VERBOSE, 0)

    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    try:
      self.curl.perform()
    except Exception as e:
      raise VcycleError('Failed uploadimg image to ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise VcycleError('Upload to ' + url + ' returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise VcycleError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')
    
    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise VcycleError('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

  def getKeyPairName(self, vmtypeName):
    """Get the key pair name from root_public_key"""

    if hasattr(self.vmtypes[vmtypeName], '_keyPairName'):
      if self.vmtypes[vmtypeName]._keyPairName:
        return self.vmtypes[vmtypeName]._keyPairName
      else:
        raise VcycleError('Key pair "' + self.vmtypes[vmtypeName].root_public_key + '" for vmtype ' + vmtypeName + ' not available!')
      
    # Get the ssh public key from the root_public_key file
        
    if self.vmtypes[vmtypeName].root_public_key[0] == '/':
      try:
        f = open(self.vmtypes[vmtypeName].root_public_key, 'r')
      except Exception as e:
        VcycleError('Cannot open ' + self.vmtypes[vmtypeName].root_public_key)
    else:  
      try:
        f = open('/var/lib/vcycle/' + self.spaceName + '/' + self.vmtypeName + '/' + self.vmtypes[vmtypeName].root_public_key, 'r')
      except Exception as e:
        VcycleError('Cannot open ' + self.spaceName + '/' + self.vmtypeName + '/' + self.vmtypes[vmtypeName].root_public_key)

    while True:
      try:
        line = f.read()
      except:
        raise VcycleError('Cannot find ssh-rsa public key line in ' + self.vmtypes[vmtypeName].root_public_key)
        
      if line[:8] == 'ssh-rsa ':
        sshPublicKey =  line.split(' ')[1]
        break

    # Check if public key is there already

    try:
      response = self.httpJSON(self.computeURL + '/os-keypairs',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for keypair in response['keypairs']:
      try:
        if 'ssh-rsa ' + sshPublicKey + ' vcycle' == keypair['keypair']['public_key']:
          self.vmtypes[vmtypeName]._keyPairName = str(keypair['keypair']['name'])
          return self.vmtypes[vmtypeName]._keyPairName
      except:
        pass
      
    # Not there so we try to add it
    
    keyName = str(time.time()).replace('.','-')

    try:
      response = self.httpJSON(self.computeURL + '/os-keypairs',
                               { 'keypair' : { 'name'       : keyName,
                                               'public_key' : 'ssh-rsa ' + sshPublicKey + ' vcycle'
                                             }
                               },
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.vmtypes[vmtypeName].root_public_key + ' in ' + self.spaceName)

    self.vmtypes[vmtypeName]._keyPairName = keyName
    return self.vmtypes[vmtypeName]._keyPairName

  def createMachine(self, vmtypeName):

    # Call the generic machine creation method
    try:
      machineName = BaseSpace.createMachine(self, vmtypeName)
    except Exception as e:
      raise VcycleError('Failed to create new machine: ' + str(e))

    # Now the OpenStack-specific machine creation steps

    try:
      request = { 'server' : 
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(vmtypeName),
                    'flavorRef' : self.getFlavorID(vmtypeName),
                    'metadata'  : { 'cern-services'   : 'false',
                                    'machinefeatures' : 'http://'  + os.uname()[1] + '/' + machineName + '/machinefeatures',
                                    'jobfeatures'     : 'http://'  + os.uname()[1] + '/' + machineName + '/jobfeatures',
                                    'machineoutputs'  : 'https://' + os.uname()[1] + '/' + machineName + '/machineoutputs' }
                  }    
                }

      if self.vmtypes[vmtypeName].root_public_key:
        request['server']['key_name'] = self.getKeyPairName(vmtypeName)

    except Exception as e:
      raise VcycleError('Failed to create new machine: ' + str(e))

    try:
      response = self.httpJSON(self.computeURL + '/servers',
                               request,
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + str(response['server']['id']) + ') for ' + vmtypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = None)

    return machineName

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

      if api.capitalize()+'Space' not in globals():
        raise VcycleError(api + ' is not a supported API for managing spaces')

      try:
        # construct an object for this space
        spaces[spaceName] = ( globals()[ api.capitalize()+'Space' ] )(api, spaceName, parser, spaceSectionName)
      except Exception as e:
        raise VcycleError('Failed to initialise space ' + spaceName + ' (' + str(e) + ')')

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
        logMachineoutputs(spaceName, vmtypeName, machineName)

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

