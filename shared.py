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

  def __init__(self, name, spaceName, state, ip, createdTime, startedTime, updatedTime, uuidStr, machinetypeName):

    # Store values from api-specific calling function
    self.name         = name
    self.spaceName    = spaceName
    self.state        = state
    self.ip           = ip
    self.createdTime  = createdTime
    self.startedTime  = startedTime
    self.updatedTime  = updatedTime
    self.uuidStr      = uuidStr
    self.machinetypeName   = machinetypeName

    if not machinetypeName:
      # Get machinetype name saved when we requested the machine
      try:
        f = open('/var/lib/vcycle/machines/' + name + '/machinetype_name', 'r')
      except:
        pass
      else:
        self.machinetypeName = f.read().strip()
        f.close()
        
    try:
      self.hs06 = float(open('/var/lib/vcycle/machines/' + name + '/machinefeatures/hs06', 'r').read().strip())
    except:
      self.hs06 = 1.0

    try:
      spaces[self.spaceName].totalMachines += 1
      spaces[self.spaceName].machinetypes[self.machinetypeName].totalMachines += 1

      if spaces[self.spaceName].machinetypes[self.machinetypeName].target_share > 0.0:
         spaces[self.spaceName].machinetypes[self.machinetypeName].weightedMachines += (self.hs06 / spaces[self.spaceName].machinetypes[self.machinetypeName].target_share)
    except:
      pass
      
    if self.state == MachineState.running:
      try:
        spaces[self.spaceName].runningMachines += 1
        spaces[self.spaceName].machinetypes[self.machinetypeName].runningMachines += 1
      except:
        pass

    try:        
      if self.state == MachineState.starting or \
         (self.state == MachineState.running and \
          ((int(time.time()) - startedTime) < spaces[self.spaceName].machinetypes[self.machinetypeName].fizzle_seconds)):
        spaces[self.spaceName].machinetypes[self.machinetypeName].notPassedFizzle += 1
    except:      
      pass

    if os.path.isdir('/var/lib/vcycle/machines/' + name):
      self.managedHere = True
    else:
      # Not managed by this Vcycle instance
      self.managedHere = False
      return
    
    # Record when the machine started (rather than just being created)
    if startedTime and not os.path.isfile('/var/lib/vcycle/machines/' + name + '/started'):
      vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + name + '/started', str(startedTime), 0600, '/var/lib/vcycle/tmp')

    try:
      self.deletedTime = int(open('/var/lib/vcycle/machines/' + name + '/deleted', 'r').read().strip())
    except:
      self.deletedTime = None

    # Set heartbeat time if available
    self.setHeartbeatTime()

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
        self.setShutdownMessage()

        if self.shutdownMessage:
          vcycle.vacutils.logLine('Machine ' + name + ' shuts down with message "' + self.shutdownMessage + '"')
          try:
            shutdownCode = int(self.shutdownMessage.split(' ')[0])
          except:
            shutdownCode = None
        else:
            shutdownCode = None
        
        if self.machinetypeName:
          # Store last abort time for stopped machines, based on shutdown message code
          if shutdownCode and \
             (shutdownCode >= 300) and \
             (shutdownCode <= 699) and \
             (self.stoppedTime > spaces[self.spaceName].machinetypes[self.machinetypeName].lastAbortTime):
            vcycle.vacutils.logLine('Set ' + self.spaceName + ' ' + self.machinetypeName + ' lastAbortTime ' + str(self.stoppedTime) + 
                                    ' due to ' + name + ' shutdown message')
            spaces[self.spaceName].machinetypes[self.machinetypeName].setLastAbortTime(self.stoppedTime)
              
          elif self.startedTime and \
               (self.stoppedTime > spaces[self.spaceName].machinetypes[self.machinetypeName].lastAbortTime) and \
               ((self.stoppedTime - self.startedTime) < spaces[self.spaceName].machinetypes[self.machinetypeName].fizzle_seconds): 

            # Store last abort time for stopped machines, based on fizzle_seconds
            vcycle.vacutils.logLine('Set ' + self.spaceName + ' ' + self.machinetypeName + ' lastAbortTime ' + str(self.stoppedTime) +
                                    ' due to ' + name + ' fizzle')
            spaces[self.spaceName].machinetypes[self.machinetypeName].setLastAbortTime(self.stoppedTime)

          if self.startedTime and shutdownCode and (shutdownCode / 100) == 3:
            vcycle.vacutils.logLine('For ' + self.spaceName + ':' + self.machinetypeName + ' minimum fizzle_seconds=' +
                                      str(self.stoppedTime - self.startedTime) + ' ?')
        
          self.writeApel()
      else:
        self.stoppedTime = None

    if self.startedTime:
      logStartedTimeStr = str(self.startedTime - self.createdTime) + 's'
    else:
      logStartedTimeStr = '-'
      
    if self.updatedTime:
      logUpdatedTimeStr = str(self.updatedTime - self.createdTime) + 's'
    else:
      logUpdatedTimeStr = '-'
    
    if self.stoppedTime:  
      logStoppedTimeStr = str(self.stoppedTime - self.createdTime) + 's'
    else:
      logStoppedTimeStr = '-'
      
    if self.heartbeatTime:
      logHeartbeatTimeStr = str(int(time.time()) - self.heartbeatTime) + 's'
    else:
      logHeartbeatTimeStr = '-'

    vcycle.vacutils.logLine('= ' + name + ' in ' + 
                            str(self.spaceName) + ':' +
                            str(self.machinetypeName) + ' ' + 
                            self.ip + ' ' + 
                            self.state + ' ' + 
                            time.strftime("%b %d %H:%M:%S ", time.localtime(self.createdTime)) + 
                            logStartedTimeStr + ':' +
                            logUpdatedTimeStr + ':' +
                            logStoppedTimeStr + ':' +                            
                            logHeartbeatTimeStr
                           )

  def writeApel(self):

    # If the VM just ran for fizzle_seconds, then we don't log it
    try:
      if (self.stoppedTime - self.startedTime) < spaces[self.spaceName].machinetypes[self.machinetypeName].fizzle_seconds:
        return
    except:
      return
        
    nowTime = time.localtime()

    userDN = ''
    for component in self.spaceName.split('.'):
      userDN = '/DC=' + component + userDN

    if hasattr(spaces[self.spaceName].machinetypes[self.machinetypeName], 'accounting_fqan'):
      userFQANField = 'FQAN: ' + spaces[self.spaceName].machinetypes[self.machinetypeName].accounting_fqan + '\n'
    else:
      userFQANField = ''
      
    if hasattr(spaces[self.spaceName].machinetypes[self.machinetypeName], 'mb'):
      memoryField = 'MemoryReal: ' + str(spaces[self.spaceName].machinetypes[self.machinetypeName].mb * 1024) + '\n' \
                    'MemoryVirtual: ' + str(spaces[self.spaceName].machinetypes[self.machinetypeName].mb * 1024) + '\n'
    else:
      memoryField = ''

    # Always true now?
    if hasattr(spaces[self.spaceName].machinetypes[self.machinetypeName], 'cpus'):
      cpusField = 'Processors: ' + str(spaces[self.spaceName].machinetypes[self.machinetypeName].cpus) + '\n'
    else:
      cpusField = ''

    if spaces[self.spaceName].gocdb_sitename:
      tmpGocdbSitename = spaces[self.spaceName].gocdb_sitename
    else:
      tmpGocdbSitename = self.spaceName

    mesg = ('APEL-individual-job-message: v0.3\n' + 
            'Site: ' + tmpGocdbSitename + '\n' +
            'SubmitHost: ' + self.spaceName + '/vcycle-' + os.uname()[1] + '\n' +
            'LocalJobId: ' + self.uuidStr + '\n' +
            'LocalUserId: ' + self.name + '\n' +
            'Queue: ' + self.machinetypeName + '\n' +
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
            'ServiceLevel: ' + str(self.hs06) + '\n' +
            '%%\n')

    fileName = time.strftime('%H%M%S', nowTime) + str(time.time() % 1)[2:][:8]
                          
    try:
      os.makedirs(time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
    except:
      pass
      
    try:
      vcycle.vacutils.createFile(time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vcycle/tmp')
    except:
      vcycle.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d/', nowTime) + fileName)
      return

    if spaces[self.spaceName].gocdb_sitename:
      # We only write to apel-outgoing if gocdb_sitename is set
      try:
        os.makedirs(time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d', nowTime), stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IXGRP|stat.S_IROTH|stat.S_IXOTH)
      except:
        pass

      try:
        vcycle.vacutils.createFile(time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH, '/var/lib/vcycle/tmp')
      except:
        vcycle.vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d/', nowTime) + fileName)
        return

  def setShutdownMessage(self):

     # Easy if a local file rather than remote
     if not spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url: 
       try:
         self.shutdownMessage = open('/var/lib/vcycle/machines/' + self.name + '/joboutputs/shutdown_message', 'r').read().strip()
       except:
         self.shutdownMessage = None

       return

     # Remote URL must be https://
     if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url[0:8] == 'https://':
       buffer = StringIO.StringIO()
       url = str(spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url + self.name + '/shutdown_message')
       spaces[self.spaceName].curl.unsetopt(pycurl.CUSTOMREQUEST)

       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '/var/lib/vcycle/machinetypes/' + self.spaceName + '/' + self.machinetypeName + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '')
          
       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '/var/lib/vcycle/machinetypes/' + self.spaceName + '/' + self.machinetypeName + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '')
          
       spaces[self.spaceName].curl.setopt(pycurl.URL, url)
       spaces[self.spaceName].curl.setopt(pycurl.NOBODY, 0)
       spaces[self.spaceName].curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
       spaces[self.spaceName].curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycleVersion)
       spaces[self.spaceName].curl.setopt(pycurl.TIMEOUT, 30)
       spaces[self.spaceName].curl.setopt(pycurl.FOLLOWLOCATION, True)
       spaces[self.spaceName].curl.setopt(pycurl.SSL_VERIFYPEER, 1)
       spaces[self.spaceName].curl.setopt(pycurl.SSL_VERIFYHOST, 2)
               
       if os.path.isdir('/etc/grid-security/certificates'):
         spaces[self.spaceName].curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')
       else:
         vcycle.vacutils.logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

       try:
         spaces[self.spaceName].curl.perform()
       except Exception as e:
         vcycle.vacutils.logLine('Failed to read ' + self.remote_joboutputs_url + self.name + '/shutdown_message (' + str(e) + ')')
         self.shutdownMessage = None
         return

       try:
         self.shutdownMessage = buffer.getvalue().strip()

         if self.shutdownMessage == '':
           self.shutdownMessage = None
       except:
         self.shutdownMessage = None

       return

     vcycle.vacutils.logLine('Problem with remote_joboutputs_url = ' + self.remote_joboutputs_url)

  def setHeartbeatTime(self):

     # Easy if a local file rather than remote
     if not spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url: 
       try:
         self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + self.name + '/joboutputs/' + spaces[self.spaceName].machinetypes[self.machinetypeName].heartbeat_file).st_ctime)
       except:
         self.heartbeatTime = None

       return

     # Remote URL must be https://
     if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url[0:8] != 'https://':
       vcycle.vacutils.logLine('Problem with remote_joboutputs_url = ' + self.remote_joboutputs_url)
     else:
       buffer = StringIO.StringIO()
       url = str(spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url + self.name + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].heartbeat_file)
       spaces[self.spaceName].curl.unsetopt(pycurl.CUSTOMREQUEST)

       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '/var/lib/vcycle/machinetypes/' + self.spaceName + '/' + self.machinetypeName + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '')
          
       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '/var/lib/vcycle/machinetypes/' + self.spaceName + '/' + self.machinetypeName + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '')
          
       spaces[self.spaceName].curl.setopt(pycurl.URL, url)
       spaces[self.spaceName].curl.setopt(pycurl.NOBODY, 1)
       spaces[self.spaceName].curl.setopt(pycurl.WRITEFUNCTION, buffer.write)
       spaces[self.spaceName].curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycleVersion)
       spaces[self.spaceName].curl.setopt(pycurl.TIMEOUT, 30)
       spaces[self.spaceName].curl.setopt(pycurl.FOLLOWLOCATION, True)
       spaces[self.spaceName].curl.setopt(pycurl.SSL_VERIFYPEER, 1)
       spaces[self.spaceName].curl.setopt(pycurl.SSL_VERIFYHOST, 2)
       spaces[self.spaceName].curl.setopt(pycurl.OPT_FILETIME, 1)
               
       if os.path.isdir('/etc/grid-security/certificates'):
         spaces[self.spaceName].curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')
       else:
         vcycle.vacutils.logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

       try:
         spaces[self.spaceName].curl.perform()
       except Exception as e:
         vcycle.vacutils.logLine('Failed to read ' + url + ' (' + str(e) + ')')
       else:

         if spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE) == 200:
           try:
             heartbeatTime = float(spaces[self.spaceName].curl.getinfo(pycurl.INFO_FILETIME))
             if heartbeatTime > 0.0:
               # Save the time we got from the remote webserver
               try:
                 open('/var/lib/vcycle/machines/' + self.name + '/vm-heartbeat', 'a')
                 os.utime('/var/lib/vcycle/machines/' + self.name + '/vm-heartbeat', (time.time(), heartbeatTime))
               except:
                 pass
           except:
             pass

         elif spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE) == 0:
             vcycle.vacutils.logLine('Fetching ' + url + ' fails with curl error ' + str(spaces[self.spaceName].curl.errstr()))
             
         elif spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE) != 404:
             vcycle.vacutils.logLine('Fetching ' + url + ' fails with HTTP response code ' + str(spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE)))

     try:
       # Use the last saved time, possibly from a previous call to this method
       self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + self.name + '/vm-heartbeat').st_mtime)
     except:
       self.heartbeatTime = None

class Machinetype:

  def __init__(self, spaceName, machinetypeName, parser, machinetypeSectionName):
  
    global maxWallclockSeconds
  
    self.spaceName  = spaceName
    self.machinetypeName = machinetypeName

    # Recreate lastAbortTime (must be set/updated with setLastAbortTime() to create file)
    try:
      f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.machinetypeName + '/last_abort_time', 'r')
    except:
      self.lastAbortTime = 0
    else:     
      self.lastAbortTime = int(f.read().strip())
      f.close()
  
    vcycle.vacutils.logLine('At ' + str(int(time.time())) + ' lastAbortTime for ' + spaceName + ':' + machinetypeName + ' set to ' + str(self.lastAbortTime))

    try:
      self.root_image = parser.get(machinetypeSectionName, 'root_image')
    except Exception as e:
      raise VcycleError('root_image is required in [' + machinetypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.flavor_name = parser.get(machinetypeSectionName, 'flavor_name')
    except Exception as e:
      raise VcycleError('flavor_name is required in [' + machinetypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.root_public_key = parser.get(machinetypeSectionName, 'root_public_key')
    except:
      self.root_public_key = None
    
    try:
      if parser.has_option(machinetypeSectionName, 'max_machines'):
        self.max_machines = int(parser.get(machinetypeSectionName, 'max_machines'))
      else:
        self.max_machines = None
    except Exception as e:
      raise VcycleError('Failed to parse max_machines in [' + machinetypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.backoff_seconds = int(parser.get(machinetypeSectionName, 'backoff_seconds'))
    except Exception as e:
      raise VcycleError('backoff_seconds is required in [' + machinetypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.fizzle_seconds = int(parser.get(machinetypeSectionName, 'fizzle_seconds'))
    except Exception as e:
      raise VcycleError('fizzle_seconds is required in [' + machinetypeSectionName + '] (' + str(e) + ')')
      
    try:
      if parser.has_option(machinetypeSectionName, 'max_wallclock_seconds'):
        self.max_wallclock_seconds = int(parser.get(machinetypeSectionName, 'max_wallclock_seconds'))
      else:
        self.max_wallclock_seconds = 86400
      
      if self.max_wallclock_seconds > maxWallclockSeconds:
        maxWallclockSeconds = self.max_wallclock_seconds
    except Exception as e:
      raise VcycleError('max_wallclock_seconds is required in [' + machinetypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.x509dn = parser.get(machinetypeSectionName, 'x509dn')
    except:
      self.x509dn = None

# The heartbeat and joboutputs options should cause errors if x509dn isn't given!
    
    try:      
      self.heartbeat_file = parser.get(machinetypeSectionName, 'heartbeat_file')
    except:
      self.heartbeat_file = None

    try:
      if parser.has_option(machinetypeSectionName, 'heartbeat_seconds'):
        self.heartbeat_seconds = int(parser.get(machinetypeSectionName, 'heartbeat_seconds'))
      else:
        self.heartbeat_seconds = None
    except Exception as e:
      raise VcycleError('Failed to parse heartbeat_seconds in [' + machinetypeSectionName + '] (' + str(e) + ')')

    if parser.has_option(machinetypeSectionName, 'log_machineoutputs') and \
               parser.get(machinetypeSectionName, 'log_machineoutputs').strip().lower() == 'true':
      self.log_joboutputs = True
      print 'log_machineoutputs is deprecated. Please use log_joboutputs'
    elif parser.has_option(machinetypeSectionName, 'log_joboutputs') and \
               parser.get(machinetypeSectionName, 'log_joboutputs').strip().lower() == 'true':
      self.log_joboutputs = True
    else:
      self.log_joboutputs = False

    if parser.has_option(machinetypeSectionName, 'machineoutputs_days'):
      print 'machineoutputs_days is deprecated. Please use joboutputs_days'

    try:
      if parser.has_option(machinetypeSectionName, 'joboutputs_days'):
        self.joboutputs_days = float(parser.get(machinetypeSectionName, 'joboutputs_days'))
      else:
        self.joboutputs_days = 3.0
    except Exception as e:
      raise VcycleError('Failed to parse joboutputs_days in [' + machinetypeSectionName + '] (' + str(e) + ')')
      
    try:      
      self.remote_joboutputs_url = parser.get(machinetypeSectionName, 'remote_joboutputs_url').rstrip('/') + '/'
    except:
      self.remote_joboutputs_url = None

    if parser.has_option(machinetypeSectionName, 'remote_joboutputs_cert') and \
                not parser.has_option(machinetypeSectionName, 'remote_joboutputs_key') :
      raise VcycleError('remote_joboutputs_cert given but remote_joboutputs_key missing (they can point to the same file if necessary)')
    elif not parser.has_option(machinetypeSectionName, 'remote_joboutputs_cert') and \
                  parser.has_option(machinetypeSectionName, 'remote_joboutputs_key') :
      raise VcycleError('remote_joboutputs_key given but remote_joboutputs_cert missing (they can point to the same file if necessary)')
    elif parser.has_option(machinetypeSectionName, 'remote_joboutputs_cert') and \
                  parser.has_option(machinetypeSectionName, 'remote_joboutputs_key') :
      self.remote_joboutputs_cert = parser.get(machinetypeSectionName, 'remote_joboutputs_cert')
      self.remote_joboutputs_key  = parser.get(machinetypeSectionName, 'remote_joboutputs_key')
    else:
      self.remote_joboutputs_cert = None
      self.remote_joboutputs_key  = None

    if parser.has_option(machinetypeSectionName, 'accounting_fqan'):
      self.accounting_fqan = parser.get(machinetypeSectionName, 'accounting_fqan').strip()

    try:
      self.hs06 = parser.get(machinetypeSectionName, 'hs06')
    except:
      self.hs06 = 1.0
  
    try:
      self.cpus = int(parser.get(machinetypeSectionName, 'cpu_per_machine'))
    except:
      self.cpus = 1
  
    try:
      self.user_data = parser.get(machinetypeSectionName, 'user_data')
    except Exception as e:
      raise VcycleError('user_data is required in [' + machinetypeSectionName + '] (' + str(e) + ')')

    try:
      if parser.has_option(machinetypeSectionName, 'target_share'):
        self.target_share = float(parser.get(machinetypeSectionName, 'target_share'))
      else:
        self.target_share = 0.0
    except Exception as e:
      raise VcycleError('Failed to parse target_share in [' + machinetypeSectionName + '] (' + str(e) + ')')

    self.options = {}
    
    for (oneOption, oneValue) in parser.items(machinetypeSectionName):
      if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):
        if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
          raise VcycleError('Name of user_data_xxx (' + oneOption + ') must only contain a-z 0-9 and _')
        else:
          self.options[oneOption] = oneValue

    if parser.has_option(machinetypeSectionName, 'user_data_proxy_cert') and \
                not parser.has_option(machinetypeSectionName, 'user_data_proxy_key') :
      raise VcycleError('user_data_proxy_cert given but user_data_proxy_key missing (they can point to the same file if necessary)')
    elif not parser.has_option(machinetypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(machinetypeSectionName, 'user_data_proxy_key') :
      raise VcycleError('user_data_proxy_key given but user_data_proxy_cert missing (they can point to the same file if necessary)')
    elif parser.has_option(machinetypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(machinetypeSectionName, 'user_data_proxy_key') :
      self.user_data_proxy_cert = parser.get(machinetypeSectionName, 'user_data_proxy_cert')
      self.user_data_proxy_key  = parser.get(machinetypeSectionName, 'user_data_proxy_key')
             
    if parser.has_option(machinetypeSectionName, 'legacy_proxy') and \
       parser.get(machinetypeSectionName, 'legacy_proxy').strip().lower() == 'true':
      self.legacy_proxy = True
    else:
      self.legacy_proxy = False

    # Just for this instance, so Total for this machinetype in one space
    self.totalMachines    = 0
    self.runningMachines  = 0
    self.weightedMachines = 0.0
    self.notPassedFizzle  = 0

  def setLastAbortTime(self, abortTime):

    if abortTime > self.lastAbortTime:
      self.lastAbortTime = abortTime

      try:
        os.makedirs('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.machinetypeName,
                    stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
      except:
        pass
        
      vcycle.vacutils.createFile('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.machinetypeName + '/last_abort_time',
                                 str(abortTime), tmpDir = '/var/lib/vcycle/tmp')

  def makeMachineName(self):
    """Construct a machine name including the machinetype"""

    return 'vcycle-' + self.machinetypeName + '-' + ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
  
class BaseSpace(object):

  def __init__(self, api, spaceName, parser, spaceSectionName):
    self.api       = api
    self.spaceName = spaceName

    try:
      self.max_machines = int(parser.get(spaceSectionName, 'max_machines'))
    except Exception as e:
      raise VcycleError('max_machines is required in [space ' + spaceName + '] (' + str(e) + ')')

    self.machinetypes = {}

    for machinetypeSectionName in parser.sections():
      try:
        (sectionType, spaceTemp, machinetypeName) = machinetypeSectionName.lower().split(None,2)
      except:
        continue

      if sectionType != 'machinetype' or spaceTemp != spaceName:
        continue

      if string.translate(machinetypeName, None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
        raise VcycleError('Name of machinetype in [machinetype ' + spaceName + ' ' + machinetypeName + '] can only contain a-z 0-9 or -')

      try:
        self.machinetypes[machinetypeName] = Machinetype(spaceName, machinetypeName, parser, machinetypeSectionName)
      except Exception as e:
        raise VcycleError('Failed to initialize [machinetype ' + spaceName + ' ' + machinetypeName + '] (' + str(e) + ')')

    if len(self.machinetypes) < 1:
      raise VcycleError('No machinetypes defined for space ' + spaceName + ' - each space must have at least one machinetype!')

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
    
    if hasattr(self, 'usercert') and hasattr(self, 'userkey') and self.usercert and self.userkey:
      if self.usercert[0] == '/':
        self.curl.setopt(pycurl.SSLCERT, self.usercert)
      else :
        self.curl.setopt(pycurl.SSLCERT, '/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.usercert)
        
      if self.userkey[0] == '/':
        self.curl.setopt(pycurl.SSLKEY, self.userkey)
      else :
        self.curl.setopt(pycurl.SSLKEY, '/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.userkey)
        
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

  def _deleteOneMachine(self, machineName):
  
    vcycle.vacutils.logLine('Deleting ' + machineName + ' in ' + self.spaceName + ':' +
                            str(self.machines[machineName].machinetypeName) + ', in state ' + str(self.machines[machineName].state))

    # record when this was tried (not when done, since don't want to overload service with failing deletes)
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/deleted', str(int(time.time())), 0600, '/var/lib/vcycle/tmp')
                                  
    # Call the subclass method specific to this space
    self.deleteOneMachine(machineName)
                                  
  def deleteMachines(self):
    # Delete machines in this space. We do not update totals here: next cycle is good enough.
      
    for machineName,machine in self.machines.iteritems():

      if not machine.managedHere:
        # We do not delete machines that are not managed by this Vcycle instance
        continue

      if machine.deletedTime and (machine.deletedTime > int(time.time()) - 3600):
        # We never try deletions more than once every 60 minutes
        continue
    
      # Delete machines as appropriate
      if machine.state == MachineState.starting and \
         (machine.createdTime is None or 
         machine.createdTime < int(time.time()) - 3600):
        # We try to delete failed-to-start machines after 60 minutes
        self._deleteOneMachine(machineName)
      elif machine.state == MachineState.failed or \
           machine.state == MachineState.shutdown or \
           machine.state == MachineState.deleting:
        # Delete non-starting, non-running machines
        self._deleteOneMachine(machineName)
      elif machine.state == MachineState.running and \
           machine.machinetypeName in self.machinetypes and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.machinetypes[machine.machinetypeName].max_wallclock_seconds)):
        self._deleteOneMachine(machineName)
      elif machine.state == MachineState.running and \
           machine.machinetypeName in self.machinetypes and \
           self.machinetypes[machine.machinetypeName].heartbeat_file and \
           self.machinetypes[machine.machinetypeName].heartbeat_seconds and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.machinetypes[machine.machinetypeName].fizzle_seconds)) and \
           (int(time.time()) > (machine.startedTime + self.machinetypes[machine.machinetypeName].heartbeat_seconds)) and \
           (
            (machine.heartbeatTime is None) or 
            (machine.heartbeatTime < (int(time.time()) - self.machinetypes[machine.machinetypeName].heartbeat_seconds))
           ):
        self._deleteOneMachine(machineName)

  def makeMachines(self):

    vcycle.vacutils.logLine('Space ' + self.spaceName + 
                            ' has ' + str(self.runningMachines) + 
                            ' running vcycle VMs out of ' + str(self.totalMachines) +
                            ' found in any state for any machinetype or none')
  
    for machinetypeName,machinetype in self.machinetypes.iteritems():
      vcycle.vacutils.logLine('machinetype ' + machinetypeName + 
                              ' has ' + str(machinetype.runningMachines) + 
                              ' running vcycle VMs out of ' + str(machinetype.totalMachines) +
                              ' found in any state. ' + str(machinetype.notPassedFizzle) +
                              ' not passed fizzle_seconds(' + str(machinetype.fizzle_seconds) +
                              ').')
  
    creationsPerCycle  = int(0.9999999 + self.max_machines * 0.1)
    creationsThisCycle = 0

    # Keep making passes through the machinetypes until limits exhausted
    while True:
      if self.totalMachines >= self.max_machines:
        vcycle.vacutils.logLine('Reached limit (%d) on number of machines to create for space %s' % (self.max_machines, self.spaceName))
        return

      if creationsThisCycle >= creationsPerCycle:
        vcycle.vacutils.logLine('Already reached limit of %d machine creations this cycle' % creationsThisCycle )
        return
      
      # For each pass, machinetypes are visited in a random order
      machinetypeNames = self.machinetypes.keys()
      random.shuffle(machinetypeNames)

      # Will record the best machinetype to create
      bestMachinetypeName = None

      for machinetypeName in machinetypeNames:
        if self.machinetypes[machinetypeName].target_share <= 0.0:
          continue

        if self.machinetypes[machinetypeName].totalMachines >= self.machinetypes[machinetypeName].max_machines:
          vcycle.vacutils.logLine('Reached limit (' + str(self.machinetypes[machinetypeName].totalMachines) + ') on number of machines to create for machinetype ' + machinetypeName)
          continue

        if int(time.time()) < (self.machinetypes[machinetypeName].lastAbortTime + self.machinetypes[machinetypeName].backoff_seconds):
          vcycle.vacutils.logLine('Free capacity found for %s ... but only %d seconds after last abort' 
                                  % (machinetypeName, int(time.time()) - self.machinetypes[machinetypeName].lastAbortTime) )
          continue

        if (int(time.time()) < (self.machinetypes[machinetypeName].lastAbortTime + 
                                self.machinetypes[machinetypeName].backoff_seconds + 
                                self.machinetypes[machinetypeName].fizzle_seconds)) and \
           (self.machinetypes[machinetypeName].notPassedFizzle > 0):
          vcycle.vacutils.logLine('Free capacity found for ' + 
                                  machinetypeName + 
                                  ' ... but still within fizzle_seconds+backoff_seconds(' + 
                                  str(int(self.machinetypes[machinetypeName].backoff_seconds + self.machinetypes[machinetypeName].fizzle_seconds)) + 
                                  ') of last abort (' + 
                                  str(int(time.time()) - self.machinetypes[machinetypeName].lastAbortTime) + 
                                  's ago) and ' + 
                                  str(self.machinetypes[machinetypeName].notPassedFizzle) + 
                                  ' starting/running but not yet passed fizzle_seconds (' + 
                                  str(self.machinetypes[machinetypeName].fizzle_seconds) + ')')
          continue

        if (not bestMachinetypeName) or (self.machinetypes[machinetypeName].weightedMachines < self.machinetypes[bestMachinetypeName].weightedMachines):
          bestMachinetypeName = machinetypeName
                 
      if bestMachinetypeName:
        vcycle.vacutils.logLine('Free capacity found for ' + bestMachinetypeName + ' within ' + self.spaceName + ' ... creating')

        # This tracks creation attempts, whether successful or not
        creationsThisCycle += 1
        self.machinetypes[machinetypeName].notPassedFizzle += 1

        try:
          self._createMachine(bestMachinetypeName)
        except Exception as e:
          vcycle.vacutils.logLine('Failed creating machine with machinetype ' + bestMachinetypeName + ' in ' + self.spaceName + ' (' + str(e) + ')')

      else:
        vcycle.vacutils.logLine('No more free capacity and/or suitable machinetype found within ' + self.spaceName)
        return
      
  def _createMachine(self, machinetypeName):
    """Generic machine creation"""
  
    try:
      machineName = self.machinetypes[machinetypeName].makeMachineName()
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
    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/joboutputs',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + 
                stat.S_IWGRP + stat.S_IXGRP + stat.S_IRGRP + 
                stat.S_IWOTH + stat.S_IXOTH + stat.S_IROTH)

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinetype_name', machinetypeName,  0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/space_name',  self.spaceName,   0644, '/var/lib/vcycle/tmp')

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/shutdown_time',
                               str(int(time.time()) + self.machinetypes[machinetypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/shutdown_time_job',
                               str(int(time.time()) + self.machinetypes[machinetypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/cpu_limit_secs',  
                               str(self.machinetypes[machinetypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/wall_limit_secs', 
                               str(self.machinetypes[machinetypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')

    if self.machinetypes[machinetypeName].remote_joboutputs_url:
      joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
    else:
      joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/' + machineName + '/joboutputs'

    try:
      userDataContents = vcycle.vacutils.createUserData(shutdownTime       = int(time.time() +
                                                                              self.machinetypes[machinetypeName].max_wallclock_seconds),
                                                        machinetypesPath   = '/var/lib/vcycle/machinetypes/' + self.spaceName,
                                                        options            = self.machinetypes[machinetypeName].options,
                                                        versionString      = 'Vcycle ' + vcycleVersion,
                                                        spaceName          = self.spaceName,
                                                        machinetypeName    = machinetypeName,
                                                        userDataPath       = self.machinetypes[machinetypeName].user_data,
                                                        hostName           = machineName + '.' + self.spaceName,
                                                        uuidStr            = None,
                                                        machinefeaturesURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/' + machineName + '/machinefeatures',
                                                        jobfeaturesURL     = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/' + machineName + '/jobfeatures',
                                                        joboutputsURL      = joboutputsURL
                                                       )
    except Exception as e:
      raise VcycleError('Failed getting user_data file (' + str(e) + ')')

    try:
      open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'w').write(userDataContents)
    except:
      raise VcycleError('Failed to writing /var/lib/vcycle/machines/' + machineName + '/user_data')

    # Call the API-specific method to actually create the machine
    try:
      self.createMachine(machineName, machinetypeName)
    except Exception as e:
      vcycle.vacutils.logLine('Machine creation fails with: ' + str(e))

    # createMachine() may determine cpus and hs06 for this machinetype, so we wait till after to set
    # those values in MJF
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/phys_cores', 
                               str(self.machinetypes[machinetypeName].cpus), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/log_cores', 
                               str(self.machinetypes[machinetypeName].cpus), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/jobslots', 
                               "1", 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/hs06', 
                               str(self.machinetypes[machinetypeName].hs06), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/allocated_CPU', 
                               str(self.machinetypes[machinetypeName].cpus), 0644, '/var/lib/vcycle/tmp')

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

      if parser.has_option(spaceSectionName, 'gocdb_sitename'):
        spaces[spaceName].gocdb_sitename = parser.get(spaceSectionName,'gocdb_sitename').strip()
      else:
        spaces[spaceName].gocdb_sitename = None

      if parser.has_option(spaceSectionName, 'https_port'):
        spaces[spaceName].https_port = int(parser.get(spaceSectionName,'https_port').strip())
      else:
        spaces[spaceName].https_port = 443

    elif sectionType != 'machinetype':
      raise VcycleError('Section type ' + sectionType + 'not recognised')

  # else: Skip over machinetype sections, which are parsed during the class initialization

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
      if spaceName not in spaces:
        # An orphaned machine from a space that is no longer configured
        # >>> We should add a proper cleanup of these machines! <<<
        continue
      elif machineName in spaces[spaceName].machines:
        # We never delete/log directories for machines that are still listed
        continue
      else:
        # If in a current space, but not listed, then delete immediately
        expireTime = 0

    # Get the time beyond which this machine shouldn't be here
    try:
      expireTime = int(open('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/shutdown_time', 'r').read().strip())
    except:
      # if the shutdown_time is missing, then we construct it using the longest lived machinetype in current config
      expireTime = int(os.stat('/var/lib/vcycle/machines/' + machineName).st_ctime) + maxWallclockSeconds

    if int(time.time()) > expireTime + 3600:

      # Get the machinetype
      try:
        machinetypeName = open('/var/lib/vcycle/machines/' + machineName + '/machinetype_name', 'r').read().strip()
      except:
        machinetypeName = None

      # Log joboutputs if a current space and machinetype and logging is enabled
      if spaceName and \
         machinetypeName and \
         spaceName in spaces and \
         machinetypeName in spaces[spaceName].machinetypes and \
         spaces[spaceName].machinetypes[machinetypeName].log_joboutputs:
        vcycle.vacutils.logLine('Saving joboutputs to /var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName)
        logJoboutputs(spaceName, machinetypeName, machineName)

      # Always delete the working copies
      try:
        shutil.rmtree('/var/lib/vcycle/machines/' + machineName)
        vcycle.vacutils.logLine('Deleted /var/lib/vcycle/machines/' + machineName)
      except:
        vcycle.vacutils.logLine('Failed deleting /var/lib/vcycle/machines/' + machineName)

def logJoboutputs(spaceName, machinetypeName, machineName):

  if os.path.exists('/var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName):
    # Copy (presumably) already exists so don't need to do anything
    return
   
  try:
    os.makedirs('/var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName,
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
  except:
    vcycle.vacutils.logLine('Failed creating /var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName)
    return

  try:
    # Get the list of files that the VM wrote in its /etc/joboutputs
    outputs = os.listdir('/var/lib/vcycle/machines/' + machineName + '/joboutputs')
  except:
    vcycle.vacutils.logLine('Failed reading /var/lib/vcycle/machines/' + machineName + '/joboutputs')
    return
        
  if outputs:
    # Go through the files one by one, adding them to the joboutputs directory
    for oneOutput in outputs:

      try:
        # first we try a hard link, which is efficient in time and space used
        os.link('/var/lib/vcycle/machines/' + machineName + '/joboutputs/' + oneOutput,
                '/var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName + '/' + oneOutput)
      except:
        try:
          # if linking failed (different filesystems?) then we try a copy
          shutil.copyfile('/var/lib/vcycle/machines/' + machineName + '/joboutputs/' + oneOutput,
                            '/var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName + '/' + oneOutput)
        except:
          vcycle.vacutils.logLine('Failed copying /var/lib/vcycle/machines/' + machineName + '/joboutputs/' + oneOutput + 
                                  ' to /var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName + '/' + oneOutput)

def cleanupJoboutputs():
  """Go through /var/lib/vcycle/joboutputs deleting expired directory trees whether they are current spaces/machinetypes or not"""

  try:
    spacesDirslist = os.listdir('/var/lib/vcycle/joboutputs/')
  except:
    return
      
  # Go through the per-machine directories
  for spaceDir in spacesDirslist:
  
    try:
      machinetypesDirslist = os.listdir('/var/lib/vcycle/joboutputs/' + spaceDir)
    except:
      continue

    for machinetypeDir in machinetypesDirslist:
        
      try:
        hostNamesDirslist = os.listdir('/var/lib/vcycle/joboutputs/' + spaceDir + '/' + machinetypeDir)
      except:
        continue
 
      for hostNameDir in hostNamesDirslist:

        # Expiration is based on file timestamp from when the COPY was created
        hostNameDirCtime = int(os.stat('/var/lib/vcycle/joboutputs/' + spaceDir + '/' + machinetypeDir + '/' + hostNameDir).st_ctime)

        try: 
          expirationDays = spaces[spaceDir].machinetypes[machinetypeDir].joboutputs_days
        except:
          # use the default if something goes wrong (configuration file changed?)
          expirationDays = 3.0
           
        if hostNameDirCtime < (time.time() - (86400 * expirationDays)):
          try:
            shutil.rmtree('/var/lib/vcycle/joboutputs/' + spaceDir + '/' + machinetypeDir + '/' + hostNameDir)
            vcycle.vacutils.logLine('Deleted /var/lib/vcycle/joboutputs/' + spaceDir + '/' + machinetypeDir + 
                                    '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')
          except:
            vcycle.vacutils.logLine('Failed deleting /var/lib/vcycle/joboutputs/' + spaceDir + '/' + 
                                    machinetypeDir + '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')

