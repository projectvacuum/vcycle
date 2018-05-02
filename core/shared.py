#!/usr/bin/python
#
#  shared.py - common functions, classes, and variables for Vcycle
#
#  Andrew McNab, Raoul Hidalgo Charman,
#  University of Manchester.
#  Copyright (c) 2013-8. All rights reserved.
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
import glob
import time
import json
import socket
import shutil
import string
import pycurl
import urllib
import random
import base64
import datetime
import StringIO
import tempfile
import calendar
import collections
import ConfigParser
import xml.etree.cElementTree

from vcycle.core import vacutils
from vcycle.core import file


file_driver = file.File(
    base_dir = '/var/lib/vcycle',
    tmp_dir = '/var/lib/vcycle/tmp')


class VcycleError(Exception):
  pass


vcycleVersion       = None
vacQueryVersion     = '01.02' # Has to match shared.py in Vac
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

  def __init__(self, name, spaceName, state, ip, createdTime, startedTime, updatedTime, uuidStr, machinetypeName, zone = None, processors = None):

    # Store values from api-specific calling function
    self.name            = name
    self.spaceName       = spaceName
    self.state           = state
    self.ip              = ip
    self.updatedTime     = updatedTime
    self.uuidStr         = uuidStr
    self.machinetypeName = machinetypeName
    self.zone            = zone

    if createdTime:
      self.createdTime  = createdTime
    else:
      try:
        # Try to recreate from created file
        self.createdTime = int(self.getFileContents('created'))
      except:
        pass

    if startedTime:
      self.startedTime = startedTime
    else:
      try:
        # Try to recreate from started file
        self.startedTime = int(self.getFileContents('started'))
      except:
        if self.state == MachineState.running:
          # If startedTime not recorded, then must just have started
          self.startedTime = int(time.time())
          self.updatedTime = self.createdTime
        else:
          self.startedTime = None

    if not self.updatedTime:
      try:
        # Try to recreate from updated file
        self.updatedTime = int(self.getFileContents('updated'))
      except:
        pass

    if not self.machinetypeName:
      # Get machinetype name saved when we requested the machine
      try:
        self.machinetypeName = self.getFileContents('machinetype_name')
      except:
        pass
      else:
        # check whether machinetype name is a managed machinetype
        if self.machinetypeName not in spaces[self.spaceName].machinetypes:
          self.machinetypeName = None

#    if not zone:
#      # Try to get zone name saved when we requested the machine
#      try:
#        f = open('/var/lib/vcycle/machines/' + name + '/zone', 'r')
#      except:
#        pass
#      else:
#        self.machinetypeName = f.read().strip()
#        f.close()

    if processors:
      self.processors = processors
    else:
      try:
        self.processors = int(self.getFileContents('jobfeatures/allocated_cpu'))
      except:
        try:
          self.processors = spaces[self.spaceName].machinetypes[self.machinetypeName].min_processors
        except:
          self.processors = 1

    try:
      self.hs06 = float(self.getFileContents('jobfeatures/hs06_job'))
    except:
      self.hs06 = None

    if os.path.isdir('/var/lib/vcycle/machines/' + name):
      self.managedHere = True
    else:
      # Not managed by this Vcycle instance
      self.managedHere = False
      return

    # Record when the machine started (rather than just being created)
    if self.startedTime and not os.path.isfile('/var/lib/vcycle/machines/' + name + '/started'):
      self.setFileContents('started', str(self.startedTime))
      self.setFileContents('updated', str(self.updatedTime))

    try:
      self.deletedTime = int(self.getFileContents('deleted'))
    except:
      self.deletedTime = None

    # Set heartbeat time if available
    self.setHeartbeatTime()

    # Check if the machine already has a stopped timestamp
    try:
      self.stoppedTime = int(self.getFileContents('stopped'))
    except:
      if (self.state == MachineState.shutdown
          or self.state == MachineState.failed
          or self.state == MachineState.deleting):
        # Record that we have seen the machine in a stopped state for the first
        # time If updateTime has the last transition time, presumably it is to
        # being stopped. This is certainly a better estimate than using
        # time.time() if available (ie OpenStack)
        if not self.updatedTime:
          self.updatedTime = int(time.time())
          self.setFileContents('updated', str(self.updatedTime))

        self.stoppedTime = self.updatedTime
        self.setFileContents('stopped', str(self.stoppedTime))

        # Record the shutdown message if available
        self.setShutdownMessage()

        if self.shutdownMessage:
          vacutils.logLine('Machine ' + name + ' shuts down with message "' + self.shutdownMessage + '"')
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
            vacutils.logLine('Set ' + self.spaceName + ' ' + self.machinetypeName + ' lastAbortTime ' + str(self.stoppedTime) +
                                    ' due to ' + name + ' shutdown message')
            spaces[self.spaceName].machinetypes[self.machinetypeName].setLastAbortTime(self.stoppedTime)

          elif self.startedTime and \
               (self.stoppedTime > spaces[self.spaceName].machinetypes[self.machinetypeName].lastAbortTime) and \
               ((self.stoppedTime - self.startedTime) < spaces[self.spaceName].machinetypes[self.machinetypeName].fizzle_seconds):

            # Store last abort time for stopped machines, based on fizzle_seconds
            vacutils.logLine('Set ' + self.spaceName + ' ' + self.machinetypeName + ' lastAbortTime ' + str(self.stoppedTime) +
                                    ' due to ' + name + ' fizzle')
            spaces[self.spaceName].machinetypes[self.machinetypeName].setLastAbortTime(self.stoppedTime)

          if self.startedTime and shutdownCode and (shutdownCode / 100) == 3:
            vacutils.logLine('For ' + self.spaceName + ':' + self.machinetypeName + ' minimum fizzle_seconds=' +
                                      str(self.stoppedTime - self.startedTime) + ' ?')

          # Machine finished messages for APEL and VacMon
          self.writeApel()
          self.sendMachineMessage()
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

    vacutils.logLine('= ' + name + ' in ' +
                            str(self.spaceName) + ':' +
                            (self.zone if self.zone else '') + ':' +
                            str(self.machinetypeName) + ' ' +
                            str(self.processors) + ' ' + self.ip + ' ' +
                            self.state + ' ' +
                            time.strftime("%b %d %H:%M:%S ", time.localtime(self.createdTime)) +
                            logStartedTimeStr + ':' +
                            logUpdatedTimeStr + ':' +
                            logStoppedTimeStr + ':' +
                            logHeartbeatTimeStr
                           )

  def getFileContents(self, fileName):
    # Get the contents of a file for this machine
    try:
      return open('/var/lib/vcycle/machines/' + self.name + '/' + fileName, 'r').read().strip()
    except:
      return None

  def setFileContents(self, fileName, contents):
    # Set the contents of a file for the given machine
    file_driver.create_file(
        'machines/' + self.name + '/' + fileName,
        contents, 0600)

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

    try:
      kb = int(self.getFileContents('jobfeatures/max_rss_bytes')) / 1024
    except:
      memoryField = ''
    else:
      memoryField = 'MemoryReal: '    + str(kb) + '\nMemoryVirtual: ' + str(kb) + '\n'

    try:
      processors = int(self.getFileContents('jobfeatures/allocated_cpu'))
    except:
      processorsField = ''
    else:
      processorsField = 'Processors: ' + str(processors) + '\n'

    if spaces[self.spaceName].gocdb_sitename:
      tmpGocdbSitename = spaces[self.spaceName].gocdb_sitename
    else:
      tmpGocdbSitename = '.'.join(self.spaceName.split('.')[1:]) if '.' in self.spaceName else self.spaceName

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
            processorsField +
            'NodeCount: 1\n' +
            'InfrastructureDescription: APEL-VCYCLE\n' +
            'InfrastructureType: grid\n' +
            'StartTime: ' + str(self.startedTime) + '\n' +
            'EndTime: ' + str(self.stoppedTime) + '\n' +
            memoryField +
            'ServiceLevelType: HEPSPEC\n' +
            'ServiceLevel: ' + str(self.hs06 if self.hs06 else 1.0) + '\n' +
            '%%\n')

    fileName = time.strftime('%H%M%S', nowTime) + str(time.time() % 1)[2:][:8]

    try:
      file_driver.create_dir(
          time.strftime('apel-archive/%Y%m%d', nowTime),
          stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP
          | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
    except:
      pass

    try:
      file_driver.create_file(
          time.strftime('apel-archive/%Y%m%d/', nowTime) + fileName, mesg,
          stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    except:
      vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-archive/%Y%m%d/', nowTime) + fileName)
      return

    if spaces[self.spaceName].gocdb_sitename:
      # We only write to apel-outgoing if gocdb_sitename is set
      try:
        file_driver.create_file(
            time.strftime('apel-outgoing/%Y%m%d', nowTime),
            stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP
            | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
      except:
        pass

      try:
        file_driver.create_file(time.strftime('apel-outgoing/%Y%m%d/', nowTime) + fileName, mesg, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IROTH)
      except:
        vacutils.logLine('Failed creating ' + time.strftime('/var/lib/vcycle/apel-outgoing/%Y%m%d/', nowTime) + fileName)
        return

  def sendMachineMessage(self, cookie = '0'):
    if not spaces[self.spaceName].vacmons:
      return

    timeNow = int(time.time())

    if spaces[self.spaceName].gocdb_sitename:
      tmpGocdbSitename = spaces[self.spaceName].gocdb_sitename
    else:
      tmpGocdbSitename = '.'.join(self.spaceName.split('.')[1:]) if '.' in self.spaceName else self.spaceName

    if not self.startedTime:
      cpuSeconds = 0
    elif self.stoppedTime:
      cpuSeconds = self.stoppedTime - self.startedTime
    elif self.state == MachineState.running:
      cpuSeconds = timeNow - self.startedTime
    else:
      cpuSeconds = 0

    messageDict = {
                'message_type'          : 'machine_status',
                'daemon_version'        : 'Vcycle ' + vcycleVersion + ' vcycled',
                'vacquery_version'      : 'VacQuery ' + vacQueryVersion,
                'cookie'                : cookie,
                'space'                 : self.spaceName,
                'site'                  : tmpGocdbSitename,
                'factory'               : os.uname()[1],
                'num_machines'          : 1,
                'time_sent'             : timeNow,

                'machine'               : self.name,
                'state'                 : self.state,
                'uuid'                  : self.uuidStr,
                'created_time'          : self.createdTime,
                'started_time'          : self.startedTime,
                'heartbeat_time'        : self.heartbeatTime,
                'num_processors'        : self.processors,
                'cpu_seconds'           : cpuSeconds,
                'cpu_percentage'        : 100.0,
                'machinetype'           : self.machinetypeName
                   }

    if self.hs06:
      messageDict['hs06'] = self.hs06

    try:
      messageDict['fqan'] = spaces[self.spaceName].machinetypes[machinetypeName].accounting_fqan
    except:
      pass

    try:
      messageDict['shutdown_message'] = self.shutdownMessage
    except:
      pass

    try:
      messageDict['shutdown_time'] = self.shutdownMessageTime
    except:
      pass

    messageJSON = json.dumps(messageDict)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    for vacmonHostPort in spaces[self.spaceName].vacmons:
      (vacmonHost, vacmonPort) = vacmonHostPort.split(':')

      vacutils.logLine('Sending VacMon machine finished message to %s:%s' % (vacmonHost, vacmonPort))

      sock.sendto(messageJSON, (vacmonHost,int(vacmonPort)))

    sock.close()

  def setShutdownMessage(self):

     self.shutdownMessage     = None
     self.shutdownMessageTime = None

     # Easy if a local file rather than remote
     if not self.machinetypeName or not spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url:
       try:
         self.shutdownMessage = open('/var/lib/vcycle/machines/' + self.name + '/joboutputs/shutdown_message', 'r').read().strip()
         self.shutdownMessageTime = int(os.stat('/var/lib/vcycle/machines/' + self.name + '/joboutputs/shutdown_message').st_ctime)
       except:
         pass

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
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '')

       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
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
         vacutils.logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

       try:
         spaces[self.spaceName].curl.perform()
       except Exception as e:
         vacutils.logLine('Failed to read ' + self.remote_joboutputs_url + self.name + '/shutdown_message (' + str(e) + ')')
         self.shutdownMessage = None
         return

       try:
         self.shutdownMessage = buffer.getvalue().strip()

         if self.shutdownMessage == '':
           self.shutdownMessage = None
       except:
         self.shutdownMessage = None

       return

     vacutils.logLine('Problem with remote_joboutputs_url = ' + self.remote_joboutputs_url)

  def setHeartbeatTime(self):
     # No valid machinetype (probably removed from configuration)
     if not self.machinetypeName:
       self.heartbeatTime = None
       return

     # Easy if a local file rather than remote
     if not spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url:
       try:
         self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + self.name + '/joboutputs/' + spaces[self.spaceName].machinetypes[self.machinetypeName].heartbeat_file).st_ctime)
       except:
         self.heartbeatTime = None

       return

     # Remote URL must be https://
     if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url[0:8] != 'https://':
       vacutils.logLine('Problem with remote_joboutputs_url = ' + self.remote_joboutputs_url)
     else:
       buffer = StringIO.StringIO()
       url = str(spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_url + self.name + '/' + spaces[self.spaceName].machinetypes[self.machinetypeName].heartbeat_file)
       spaces[self.spaceName].curl.unsetopt(pycurl.CUSTOMREQUEST)

       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_cert)
       except:
          spaces[self.spaceName].curl.setopt(pycurl.SSLCERT, '')

       try:
        if spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key[0] == '/':
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
        else:
          spaces[self.spaceName].curl.setopt(pycurl.SSLKEY, '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + spaces[self.spaceName].machinetypes[self.machinetypeName].remote_joboutputs_key)
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
         vacutils.logLine('/etc/grid-security/certificates directory does not exist - relying on curl bundle of commercial CAs')

       try:
         spaces[self.spaceName].curl.perform()
       except Exception as e:
         vacutils.logLine('Failed to read ' + url + ' (' + str(e) + ')')
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
             vacutils.logLine('Fetching ' + url + ' fails with curl error ' + str(spaces[self.spaceName].curl.errstr()))

         elif spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE) != 404:
             vacutils.logLine('Fetching ' + url + ' fails with HTTP response code ' + str(spaces[self.spaceName].curl.getinfo(pycurl.RESPONSE_CODE)))

     try:
       # Use the last saved time, possibly from a previous call to this method
       self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + self.name + '/vm-heartbeat').st_mtime)
     except:
       self.heartbeatTime = None


class Machinetype:

  def __init__(self, spaceName, spaceFlavorNames, machinetypeName, parser, machinetypeSectionName):

    global maxWallclockSeconds

    self.spaceName  = spaceName
    self.machinetypeName = machinetypeName

    # Recreate lastAbortTime (must be set/updated with setLastAbortTime() to create file)
    try:
      f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/last_abort_time', 'r')
    except:
      self.lastAbortTime = 0
    else:
      self.lastAbortTime = int(f.read().strip())
      f.close()

    # Always set machinetype_path, saved in vacuum pipe processing or default using machinetype name
    try:
      self.machinetype_path = parser.get(machinetypeSectionName, 'machinetype_path')
    except:
      self.machinetype_path = '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' +  self.machinetypeName

    try:
      self.root_image = parser.get(machinetypeSectionName, 'root_image')
    except:
      self.root_image = None

    try:
      self.cernvm_signing_dn = parser.get(machinetypeSectionName, 'cernvm_signing_dn')
    except:
      self.cernvm_signing_dn = None

    if parser.has_option(machinetypeSectionName, 'flavor_name'):
      vacutils.logLine('Option flavor_name is deprecated, please use flavor_names!')
      try:
        self.flavor_names = parser.get(machinetypeSectionName, 'flavor_name').strip().split()
      except:
        self.flavor_names = spaceFlavorNames
    else:
      try:
        self.flavor_names = parser.get(machinetypeSectionName, 'flavor_names').strip().split()
      except:
        self.flavor_names = spaceFlavorNames

    try:
      self.min_processors = int(parser.get(machinetypeSectionName, 'cpu_per_machine'))
    except:
      pass
    else:
      vacutils.logLine('cpu_per_machine is deprecated - please use min_processors')

    try:
      self.min_processors = int(parser.get(machinetypeSectionName, 'processors_per_machine'))
    except:
      pass
    else:
      vacutils.logLine('processors_per_machine is deprecated - please use min_processors')

    try:
      self.min_processors = int(parser.get(machinetypeSectionName, 'min_processors'))
    except Exception as e:
      self.min_processors = 1

    try:
      self.max_processors = int(parser.get(machinetypeSectionName, 'max_processors'))
    except Exception as e:
      self.max_processors = None
      
    if self.max_processors is not None and self.max_processors < self.min_processors:
      raise VcycleError('max_processors cannot be less than min_processors!')
        
    try:
      self.disk_gb_per_processor = int(parser.get(machinetypeSectionName, 'disk_gb_per_processor'))
    except Exception as e:
      self.disk_gb_per_processor = None

    try:
      self.root_public_key = parser.get(machinetypeSectionName, 'root_public_key')
    except:
      self.root_public_key = None

    try:
      if parser.has_option(machinetypeSectionName, 'processors_limit'):
        self.processors_limit = int(parser.get(machinetypeSectionName, 'processors_limit'))
      else:
        self.processors_limit = None
    except Exception as e:
      raise VcycleError('Failed to parse processors_limit in [' + machinetypeSectionName + '] (' + str(e) + ')')

    if parser.has_option(machinetypeSectionName, 'max_starting_processors'):
      try:
        self.max_starting_processors = int(parser.get(machinetypeSectionName, 'max_starting_processors'))
      except Exception as e:
        raise VcycleError('Failed to parse max_starting_processors in [' + machinetypeSectionName + '] (' + str(e) + ')')
    else:
      self.max_starting_processors = self.processors_limit

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

    try:
      s = parser.get(machinetypeSectionName, 'cvmfs_proxy_machinetype')
    except:
      self.cvmfsProxyMachinetype     = None
      self.cvmfsProxyMachinetypePort = None
    else:
      if ':' in s:
        try:
          self.cvmfsProxyMachinetype = s.split(':')[0]
          self.cvmfsProxyMachinetypePort = int(s.split(':')[1])
        except: 
          raise VcycleError('Failed to parse cmvfs_proxy_machinetype = ' + s + ' in [' + machinetypeSectionName + '] (' + str(e) + ')')
      else:
        self.cvmfsProxyMachinetype     = s
        self.cvmfsProxyMachinetypePort = 280

    if parser.has_option(machinetypeSectionName, 'log_machineoutputs') and \
               parser.get(machinetypeSectionName, 'log_machineoutputs').lower() == 'true':
      self.log_joboutputs = True
      vacutils.logLine('log_machineoutputs is deprecated. Please use log_joboutputs')
    elif parser.has_option(machinetypeSectionName, 'log_joboutputs') and \
               parser.get(machinetypeSectionName, 'log_joboutputs').lower() == 'true':
      self.log_joboutputs = True
    else:
      self.log_joboutputs = False

    if parser.has_option(machinetypeSectionName, 'machineoutputs_days'):
      vacutils.logLine('machineoutputs_days is deprecated. Please use joboutputs_days')

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
      self.accounting_fqan = parser.get(machinetypeSectionName, 'accounting_fqan')

    try:
      self.rss_bytes_per_processor = 1048576 * int(parser.get(machinetypeSectionName, 'mb_per_processor'))
    except:
      # If not set explicitly, defaults to 2048 MB per processor
      self.rss_bytes_per_processor = 2147483648

    if parser.has_option(machinetypeSectionName, 'hs06_per_processor'):
      try:
        self.hs06_per_processor = float(parser.get(machinetypeSectionName, 'hs06_per_processor'))
      except Exception as e:
        VcycleError('Failed to parse hs06_per_processor in [' + machinetypeSectionName + '] (' + str(e) + ')')
      else:
        self.runningHS06 = 0.0
    else:
      self.hs06_per_processor = None
      self.runningHS06 = None

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

    # self.options will be passed to vacutils.createUserData()
    self.options = {}

    for (oneOption, oneValue) in parser.items(machinetypeSectionName):
      if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):
        if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
          raise VcycleError('Name of user_data_xxx (' + oneOption + ') must only contain a-z 0-9 and _')
        else:
          self.options[oneOption] = oneValue

    if parser.has_option(machinetypeSectionName, 'user_data_proxy_cert') or \
       parser.has_option(machinetypeSectionName, 'user_data_proxy_key') :
      vacutils.logLine('user_data_proxy_cert and user_data_proxy_key are deprecated. Please use user_data_proxy = True and create x509cert.pem and x509key.pem!')

    if parser.has_option(machinetypeSectionName, 'user_data_proxy') and \
       parser.get(machinetypeSectionName,'user_data_proxy').lower() == 'true':
      self.options['user_data_proxy'] = True
    else:
      self.options['user_data_proxy'] = False    

    if parser.has_option(machinetypeSectionName, 'legacy_proxy') and \
       parser.get(machinetypeSectionName, 'legacy_proxy').lower() == 'true':
      self.options['legacy_proxy'] = True
    else:
      self.options['legacy_proxy'] = False
    
    # Just for this instance, so Total for this machinetype in one space
    self.totalMachines      = 0
    self.totalProcessors    = 0
    self.startingProcessors = 0
    self.runningMachines    = 0
    self.runningProcessors  = 0
    self.weightedMachines   = 0.0
    self.notPassedFizzle    = 0

  def setLastAbortTime(self, abortTime):

    if abortTime > self.lastAbortTime:
      self.lastAbortTime = abortTime

      try:
        file_driver.create_dir(
            'spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName,
            stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP
            + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
      except:
        pass

      file_driver.create_file(
          'spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName
          + '/last_abort_time', str(abortTime))

  def makeMachineName(self):
    """Construct a machine name including the machinetype"""

    while True:
      machineName = 'vcycle-' + self.machinetypeName + '-' + ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))      

      if not os.path.exists('/var/lib/vcycle/machines/' + machineName):
        break
  
      vacutils.logLine('New random machine name ' + machineName + ' already exists! Trying another name ...')

    return machineName


class BaseSpace(object):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
    self.api        = api
    self.apiVersion = apiVersion
    self.spaceName  = spaceName

    self.processors_limit   = None
    self.totalMachines      = 0
    # totalProcessors includes ones Vcycle doesn't manage
    self.totalProcessors    = 0
    self.runningMachines    = 0
    self.runningProcessors  = 0
    self.runningHS06        = None
    self.zones              = None
    self.maxStartingSeconds = 3600
    self.shutdownTime  = None

    if parser.has_option(spaceSectionName, 'max_processors'):
      vacutils.logLine('max_processors (in space ' + spaceName + ') is deprecated - please use processors_limit')
      try:
        self.processors_limit = int(parser.get(spaceSectionName, 'max_processors'))
      except:
        raise VcycleError('Failed to parse max_processors in [space ' + spaceName + '] (' + str(e) + ')')
      
    elif parser.has_option(spaceSectionName, 'processors_limit'):
      try:
        self.processors_limit = int(parser.get(spaceSectionName, 'processors_limit'))
      except Exception as e:
        raise VcycleError('Failed to parse processors_limit in [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.flavor_names = parser.get(spaceSectionName, 'flavor_names').strip().split()
    except:
      self.flavor_names = []

    if parser.has_option(spaceSectionName, 'shutdown_time'):
      try:
        self.shutdownTime = int(parser.get(spaceSectionName,
          'shutdown_time'))
      except Exception as e:
        raise VcycleError('Failed to check parse shutdown_time in ['
            + spaceSectionName + '] (' + str(e) + ')')

    # First go through the vacuum_pipe sections for this space, creating
    # machinetype sections in the configuration on the fly
    for vacuumPipeSectionName in parser.sections():
      try:
        (sectionType, spaceTemp, machinetypeNamePrefix) = vacuumPipeSectionName.lower().split(None,2)
      except:
        continue

      if spaceTemp != spaceName or sectionType != 'vacuum_pipe':
        continue

      try:
        self._expandVacuumPipe(parser, vacuumPipeSectionName, machinetypeNamePrefix, updatePipes)
      except Exception as e:
        raise VcycleError('Failed expanding vacuum pipe [' + vacuumPipeSectionName + ']: ' + str(e))

    # Now go through the machinetypes for this space in the configuration,
    # possibly including ones created from vacuum pipes
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
        self.machinetypes[machinetypeName] = Machinetype(spaceName, self.flavor_names, machinetypeName, parser, machinetypeSectionName)
      except Exception as e:
        raise VcycleError('Failed to initialize [machinetype ' + spaceName + ' ' + machinetypeName + '] (' + str(e) + ')')

      if self.runningHS06 is None and self.machinetypes[machinetypeName].hs06_per_processor is not None:
        self.runningHS06 = 0.0

    if len(self.machinetypes) < 1:
      raise VcycleError('No machinetypes defined for space ' + spaceName + ' - each space must have at least one machinetype!')

    # Start new curl session for this instance
    self.curl  = pycurl.Curl()
    self.token = None

    # all the Vcycle-created VMs in this space
    self.machines = {}

    if parser.has_option(spaceSectionName, 'gocdb_sitename'):
      self.gocdb_sitename = parser.get(spaceSectionName,'gocdb_sitename')
    else:
      self.gocdb_sitename = None

    if parser.has_option(spaceSectionName, 'gocdb_cert_file'):
      self.gocdb_cert_file = parser.get(spaceSectionName,'gocdb_cert_file')
    else:
      self.gocdb_cert_file = None

    if parser.has_option(spaceSectionName, 'gocdb_key_file'):
      self.gocdb_key_file = parser.get(spaceSectionName,'gocdb_key_file')
    else:
      self.gocdb_key_file = None
      
    if self.gocdb_cert_file and self.gocdb_key_file is None:
      raise VcycleError('gocdb_cert_file given but gocdb_key_file is missing!')

    if self.gocdb_cert_file is None and self.gocdb_key_file:
      raise VcycleError('gocdb_key_file given but gocdb_cert_file is missing!')

    if parser.has_option(spaceSectionName, 'vacmon_hostport'):
      try:
        self.vacmons = parser.get(spaceSectionName,'vacmon_hostport').lower().split()
      except:
        raise VcycleError('Failed to parse vacmon_hostport for space ' + spaceName)

      for v in self.vacmons:
        if re.search('^[a-z0-9.-]+:[0-9]+$', v) is None:
          raise VcycleError('Failed to parse vacmon_hostport: must be host.domain:port')
    else:
      self.vacmons = []

    if parser.has_option(spaceSectionName, 'https_port'):
      self.https_port = int(parser.get(spaceSectionName,'https_port').strip())
    else:
      self.https_port = 443


  def _expandVacuumPipe(self, parser, vacuumPipeSectionName, machinetypeNamePrefix, updatePipes):
    """ Read configuration settings from a vacuum pipe """

    acceptedOptions = [
        'accounting_fqan',
        'backoff_seconds',
        'cache_seconds',
        'cvmfs_repositories',
        'fizzle_seconds',
        'heartbeat_file',
        'heartbeat_seconds',
        'image_signing_dn',
        'legacy_proxy',
        'machine_model',
        'max_processors',
        'max_wallclock_seconds',
        'min_processors',
        'min_wallclock_seconds',
        'root_device',
        'root_image',
        'scratch_device',
        'suffix',
        'target_share',
        'user_data',
        'user_data_proxy'
        ]

    try:
      vacuumPipeURL = parser.get(vacuumPipeSectionName, 'vacuum_pipe_url')
    except:
      raise VcycleError('Section vacuum_pipe ' + machinetypeNamePrefix + ' in space ' + spaceName + ' has no vacuum_pipe_url option!')

    # This is the total in the local configuation, for this pipe and its machinetypes
    try:
      totalTargetShare = float(parser.get(vacuumPipeSectionName, 'target_share').strip())
    except:
      totalTargetShare = 0.0

    try:
      vacuumPipe = vacutils.readPipe(
          '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/'
          + machinetypeNamePrefix + '/vacuum.pipe',
          vacuumPipeURL,
          'vcycle ' + vcycleVersion,
          updatePipes = updatePipes)
    except Exception as e:
      raise VcycleError(vacuumPipeURL + ' given but failed reading/updating the pipe: ' + str(e))

    # This is the total in the remote pipe file, for the machinetypes it defines
    totalPipeTargetShare = 0.0
              
    # First pass to get total target shares in the remote vacuum pipe
    for pipeMachinetype in vacuumPipe['machinetypes']:
      try:
        totalPipeTargetShare += float(pipeMachinetype['target_share'])
      except:
        pass

    # Second pass to add options to the relevant machinetype sections
    for pipeMachinetype in vacuumPipe['machinetypes']:
    
      if 'machine_model' in pipeMachinetype and str(pipeMachinetype['machine_model']) not in ['cernvm3','vm-raw']:
        vacutils.logLine("Not a supported machine_model: %s - skipping!" % str(pipeMachinetype['machine_model']))
        continue    

      try:
        suffix = str(pipeMachinetype['suffix'])
      except:
        vacutils.logLine("suffix is missing from one machinetype within " + vacuumPipeURL + " - skipping!")
        continue
                
      try:
        parser.add_section('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix)
      except:
        # Ok if it already exists
        pass

      # Copy almost all options from vacuum_pipe section to this new machinetype
      # unless they have already been given. Skip vacuum_pipe_url and target_share                  
      for n,v in parser.items(vacuumPipeSectionName):
        if n != 'vacuum_pipe_url' and n != 'target_share' and \
           not parser.has_option('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix, n):
          parser.set('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix, n, v)

      # Record path to machinetype used to find the files on local disk
      parser.set('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix,
                 'machinetype_path', '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' +  machinetypeNamePrefix)      

      # Go through vacuumPipe adding options if not already present from configuration files
      for optionRaw in pipeMachinetype:
        option = str(optionRaw)
        value  = str(pipeMachinetype[optionRaw])

        # Skip if option already exists for this machinetype - configuration 
        # file sections take precedence
        if parser.has_option('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix, option):
          continue
        
        # Deal with subdividing the total target share for this vacuum pipe here
        # Each machinetype gets a share based on its target_share within the pipe
        # We do the normalisation of the pipe target_shares here
        if option == 'target_share':
          try:
            targetShare = totalTargetShare * (float(value) / totalPipeTargetShare)
          except:
            targetShare = 0.0

          parser.set('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix, 
                     'target_share', str(targetShare))
          continue

        # Silently skip some options processed already
        if option == 'machine_model':
          continue

        # Check option is one we accept
        if not option.startswith('user_data_file_' ) and \
           not option.startswith('user_data_option_' ) and \
           not option in acceptedOptions:
          vacutils.logLine('Option %s is not accepted from vacuum pipe - ignoring!' % option)
          continue

        # Any options which specify filenames on the hypervisor must be checked here
        if (option.startswith('user_data_file_' )  or
            option ==         'heartbeat_file'   ) and '/' in value:
          vacutils.logLine('Option %s in %s cannot contain a "/" - ignoring!'
             % (option, vacuumPipeURL))
          continue

        elif (option == 'user_data' or option == 'root_image') and '/../' in value:
          vacutils.logLine('Option %s in %s cannot contain "/../" - ignoring!'
             % (option, vacuumPipeURL))
          continue

        elif option == 'user_data' and '/' in value and \
           not value.startswith('http://') and \
           not value.startswith('https://'):
          vacutils.logLine('Option %s in %s cannot contain a "/" unless http(s)://... - ignoring!'
             % (option, vacuumPipeURL))
          continue

        elif option == 'root_image' and '/' in value and \
           not value.startswith('http://') and \
           not value.startswith('https://'):
          vacutils.logLine('Option %s in %s cannot contain a "/" unless http(s)://... - ignoring!'
             % (option, vacuumPipeURL))
          continue
          
        # if all OK, then can set value as if from configuration files
        parser.set('machinetype ' + self.spaceName + ' ' + machinetypeNamePrefix + '-' + suffix, 
                   option, value)

  def findMachinesWithFile(self, fileName):
    # Return a list of machine names that have the given fileName

    machineNames = []
    pathsList    = glob.glob('/var/lib/vcycle/machines/*/' + fileName)

    if pathsList:
      for onePath in pathsList:
        machineNames.append(onePath.split('/')[5])

    return machineNames

  def getFileContents(self, machineName, fileName):
    # Get the contents of a file for the given machine
    try:
      return open('/var/lib/vcycle/machines/' + machineName + '/' + fileName, 'r').read().strip()
    except:
      return None

  def setFileContents(self, machineName, fileName, contents):
    # Set the contents of a file for the given machine
    open('/var/lib/vcycle/machines/' + machineName + '/' + fileName, 'w').write(contents)

  def connect(self):
    # Null method in case this API doesn't need a connect step
    pass

  def _xmlToDictRecursor(self, xmlTree):

    tag      = xmlTree.tag.split('}')[1]
    retDict  = { tag: {} }
    children = list(xmlTree)

    if children:
      childrenLists = collections.defaultdict(list)

      for recursorDict in map(self._xmlToDictRecursor, children):
        for key, value in recursorDict.iteritems():
          childrenLists[key].append(value)

      childrenDict = {}
      for key, value in childrenLists.iteritems():
         # Value is always a list, so ask for value[0] even if single child
         childrenDict[key] = value

      retDict = { tag: childrenDict }

    if xmlTree.attrib:
      retDict[tag].update(('@' + key, value) for key, value in xmlTree.attrib.iteritems())

    if xmlTree.text and xmlTree.text.strip():
      retDict[tag]['#text'] = xmlTree.text.strip()

    return retDict

  def _xmlToDict(self, xmlString):
    # Convert XML string to dictionary
    # - Each tag below root has a list of dictionaries as its value even if length 1 (or 0!)
    # - Text contained within the tag itself appears as key #text
    # - Attributes of the tag appear as key @attributename
    return self._xmlToDictRecursor(xml.etree.cElementTree.XML(xmlString))

  def httpRequest(self,
                  url, 			# HTTP(S) URL to contact
                  request = None, 	# = jsonRequest for compatibility
                  jsonRequest = None, 	# dictionary to be converted to JSON body (overrides formRequest)
                  formRequest = None,   # dictionary to be converted into HTML Form body, or body itself
                  headers = None, 	# request headers
                  verbose = False, 	# turn on Curl logging messages
                  method = None, 	# DELETE, otherwise always GET/POST
                  anyStatus = False	# accept any HTTP status without exception, not just 2xx
                 ):

    # Returns dictionary:  { 'headers' : HEADERS, 'response' : DICTIONARY, 'raw' : string, 'status' : CURL RESPONSE CODE }

    self.curl.unsetopt(pycurl.CUSTOMREQUEST)
    self.curl.setopt(pycurl.URL, str(url))
    self.curl.setopt(pycurl.USERAGENT, 'Vcycle ' + vcycleVersion)

    # backwards compatible
    if request:
      jsonRequest = request

    if method and method.upper() == 'DELETE':
      self.curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
    elif jsonRequest:
      try:
        self.curl.setopt(pycurl.POSTFIELDS, json.dumps(jsonRequest))
      except Exception as e:
        raise VcycleError('JSON encoding of "' + str(jsonRequest) + '" fails (' + str(e) + ')')
    elif formRequest:

      if isinstance(formRequest, dict):
        # if formRequest is a dictionary then encode it
        try:
          self.curl.setopt(pycurl.POSTFIELDS, urllib.urlencode(formRequest))
        except Exception as e:
          raise VcycleError('Form encoding of "' + str(formRequest) + '" fails (' + str(e) + ')')
      else:
        # otherwise assume formRequest is already formatted
        try:
          self.curl.setopt(pycurl.POSTFIELDS, formRequest)
        except Exception as e:
          raise VcycleError('Form encoding of "' + str(formRequest) + '" fails (' + str(e) + ')')

    else :
      # No body, just GET and headers
      self.curl.setopt(pycurl.HTTPGET, True)

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)

    headersBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.HEADERFUNCTION, headersBuffer.write)

    # Set up the list of headers to send in the request
    allHeaders = []

    if jsonRequest:
      allHeaders.append('Content-Type: application/json')
      allHeaders.append('Accept: application/json')
    elif formRequest:
      allHeaders.append('Content-Type: application/x-www-form-urlencoded')

    if headers:
      allHeaders.extend(headers)

    self.curl.setopt(pycurl.HTTPHEADER, allHeaders)

    if verbose:
      self.curl.setopt(pycurl.VERBOSE, 2)
    else:
      self.curl.setopt(pycurl.VERBOSE, 0)

    self.curl.setopt(pycurl.TIMEOUT,        30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
    self.curl.setopt(pycurl.SSLVERSION,     pycurl.SSLVERSION_TLSv1)

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
    outputHeaders = { }

    while True:

      try:
        oneLine = headersBuffer.readline().strip()
      except:
        break

      if not oneLine:
        break

      if oneLine.startswith('HTTP/1.1 '):
        # An HTTP return code, overwrite any previous code
        outputHeaders['status'] = [ oneLine[9:] ]

        if oneLine == 'HTTP/1.1 100 Continue':
          # Silently eat the blank line
          oneLine = headersBuffer.readline().strip()

      else:
        # Otherwise a Name: Value header
        headerNameValue = oneLine.split(':',1)

        # outputHeaders is a dictionary of lowercased header names
        # but the values are always lists, with one or more values (if multiple headers with the same name)
        if headerNameValue[0].lower() not in outputHeaders:
          outputHeaders[ headerNameValue[0].lower() ] = []

        outputHeaders[ headerNameValue[0].lower() ].append( headerNameValue[1].strip() )

    if 'content-type' in outputHeaders and outputHeaders['content-type'][0].startswith('application/json'):
      try:
        response = json.loads(outputBuffer.getvalue())
      except:
        response = None

    elif 'content-type' in outputHeaders and outputHeaders['content-type'][0] == 'text/xml':
      try:
        response = self._xmlToDict(outputBuffer.getvalue())
      except:
        response = None

    else:
      response = None

    # If not a 2xx code then raise an exception unless anyStatus option given
    if not anyStatus and self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      try:
        vacutils.logLine('Query raw response: ' + str(outputBuffer.getvalue()))
      except:
        pass

      raise VcycleError('Query of ' + url + ' returns HTTP code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    return { 'headers' : outputHeaders, 'response' : response, 'raw' : str(outputBuffer.getvalue()), 'status' : self.curl.getinfo(pycurl.RESPONSE_CODE) }

  def _deleteOneMachine(self, machineName, shutdownMessage = None):

    vacutils.logLine('Deleting ' + machineName + ' in ' + self.spaceName + ':' +
                            str(self.machines[machineName].machinetypeName) + ', in state ' + str(self.machines[machineName].state))

    # record when this was tried (not when done, since don't want to overload service with failing deletes)
    file_driver.create_file('machines/' + machineName + '/deleted', str(int(time.time())), 0600)

    if shutdownMessage and not os.path.exists('/var/lib/vcycle/machines/' + machineName + '/joboutputs/shutdown_message'):
      try:
        file_driver.create_file('machines/' + machineName + '/joboutputs/shutdown_message', shutdownMessage, 0600)
      except:
        pass

    # Call the subclass method specific to this space
    self.deleteOneMachine(machineName)

  def deleteMachines(self):
    # Delete machines in this space. We do not update totals here: next cycle is good enough.

    for machineName, machine in self.machines.iteritems():

      if not machine.managedHere:
        # We do not delete machines that are not managed by this Vcycle instance
        continue

      if machine.deletedTime and (machine.deletedTime > int(time.time()) - 3600):
        # We never try deletions more than once every 60 minutes
        continue

      # Delete machines as appropriate
      if machine.state == MachineState.starting and \
         (machine.createdTime is None or
          (self.maxStartingSeconds and
           machine.createdTime < int(time.time()) - self.maxStartingSeconds)):
        # We try to delete failed-to-start machines after maxStartingSeconds (default 3600)
        self._deleteOneMachine(machineName, '700 Failed to start')

      elif machine.state == MachineState.failed or \
           machine.state == MachineState.shutdown or \
           machine.state == MachineState.deleting:
        # Delete non-starting, non-running machines
        self._deleteOneMachine(machineName)

      elif machine.state == MachineState.running and \
           machine.machinetypeName in self.machinetypes and \
           machine.startedTime and \
           (int(time.time()) > (machine.startedTime + self.machinetypes[machine.machinetypeName].max_wallclock_seconds)):
        vacutils.logLine(machineName + ' exceeded max_wallclock_seconds')
        self._deleteOneMachine(machineName, '700 Exceeded max_wallclock_seconds')

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
        vacutils.logLine(machineName + ' failed to update heartbeat file')
        self._deleteOneMachine(machineName, '700 Heartbeat file not updated')

      # Check shutdown times
      elif machine.state == MachineState.running and \
           machine.machinetypeName in self.machinetypes:
        shutdowntime = self.updateShutdownTime(machine)
        if shutdowntime is not None and int(time.time()) > shutdowntime:
          # log what has passed
          if self.shutdownTime == shutdowntime:
            vacutils.logLine(
                'shutdown time ({}) for space {} has passed'
                .format(shutdowntime, self.spaceName))
          else:
            vacutils.logLine(
                'shutdown time ({}) for machine {} has passed'
                .format(shutdowntime, machineName))
          self._deleteOneMachine(machineName, '700 Passed shutdowntime')

  def createHeartbeatMachines(self):
    # Create a list of machines in each machinetype, to be populated
    # with machine names of machines with a current heartbeat
    try:
      file_driver.create_dir('spaces/' + self.spaceName + '/heartbeatmachines',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP
                + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    except:
      pass

    for machinetypeName in self.machinetypes:
       self.machinetypes[machinetypeName].heartbeatMachines = []

    for machineName,machine in self.machines.iteritems():
      if machine.managedHere and \
         machine.state == MachineState.running and \
         machine.machinetypeName in self.machinetypes and \
         self.machinetypes[machine.machinetypeName].heartbeat_file and \
         self.machinetypes[machine.machinetypeName].heartbeat_seconds and \
         machine.startedTime and \
         (
          (machine.heartbeatTime is not None) and
          (machine.heartbeatTime > (int(time.time()) - self.machinetypes[machine.machinetypeName].heartbeat_seconds))
         ):
        # An active machine producing its heartbeat
        self.machinetypes[machine.machinetypeName].heartbeatMachines.append(machineName)

    # Save these lists as files accessible through the web server    
    for machinetypeName in self.machinetypes:
      fileContents = []
      for machineName in self.machinetypes[machinetypeName].heartbeatMachines:
        fileContents.append('%d %s %s\n' 
                        % (self.machines[machineName].heartbeatTime, machineName, self.machines[machineName].ip))

      # Sort the list by heartbeat time, newest first, then write as a file
      fileContents.sort(reverse=True)
      file_driver.create_file('spaces/' + self.spaceName + '/heartbeatmachines/' + machinetypeName, ''.join(fileContents), 0664)
      
  def makeFactoryMessage(self, cookie = '0'):
    factoryHeartbeatTime = int(time.time())

    try:
      mjfHeartbeatTime = int(os.stat('/var/log/httpd/https-vcycle.log').st_ctime)
      metadataHeartbeatTime = mjfHeartbeatTime
    except:
      mjfHeartbeatTime = 0
      metadataHeartbeatTime = 0

    try:
      bootTime = int(time.time() - float(open('/proc/uptime','r').readline().split()[0]))
    except:
      bootTime = 0

    daemonDiskStatFS  = os.statvfs('/var/lib/vcycle')
    rootDiskStatFS = os.statvfs('/tmp')

    memory = vacutils.memInfo()

    try:
      osIssue = open('/etc/issue.vac','r').readline().strip()
    except:
      try:
        osIssue = open('/etc/issue','r').readline().strip()
      except:
        osIssue = os.uname()[2]

    if self.gocdb_sitename:
      tmpGocdbSitename = self.gocdb_sitename
    else:
      tmpGocdbSitename = '.'.join(self.spaceName.split('.')[1:]) if '.' in self.spaceName else self.spaceName

    messageDict = {
                'message_type'             : 'factory_status',
                'daemon_version'           : 'Vcycle ' + vcycleVersion + ' vcycled',
                'vacquery_version'         : 'VacQuery ' + vacQueryVersion,
                'cookie'                   : cookie,
                'space'                    : self.spaceName,
                'site'                     : tmpGocdbSitename,
                'factory'                  : os.uname()[1],
                'time_sent'                : int(time.time()),

                'running_processors'       : self.runningProcessors,
                'running_machines'         : self.runningMachines,

                'max_processors'           : self.processors_limit,
                'max_machines'             : self.processors_limit,

                'root_disk_avail_kb'       : (rootDiskStatFS.f_bavail * rootDiskStatFS.f_frsize) / 1024,
                'root_disk_avail_inodes'   : rootDiskStatFS.f_favail,

                'daemon_disk_avail_kb'      : (daemonDiskStatFS.f_bavail *  daemonDiskStatFS.f_frsize) / 1024,
                'daemon_disk_avail_inodes'  : daemonDiskStatFS.f_favail,

                'load_average'             : vacutils.loadAvg(2),
                'kernel_version'           : os.uname()[2],
                'os_issue'                 : osIssue,
                'boot_time'                : bootTime,
                'factory_heartbeat_time'   : factoryHeartbeatTime,
                'mjf_heartbeat_time'       : mjfHeartbeatTime,
                'metadata_heartbeat_time'  : metadataHeartbeatTime,
                'swap_used_kb'             : memory['SwapTotal'] - memory['SwapFree'],
                'swap_free_kb'             : memory['SwapFree'],
                'mem_used_kb'              : memory['MemTotal'] - memory['MemFree'],
                'mem_total_kb'             : memory['MemTotal']
                  }

    if self.runningHS06 is not None:
      messageDict['max_hs06']     = self.runningHS06
      messageDict['running_hs06'] = self.runningHS06

    return json.dumps(messageDict)

  def makeMachinetypeMessages(self, cookie = '0'):
    messages = []
    timeNow = int(time.time())
    numMachinetypes = len(spaces[self.spaceName].machinetypes)

    if spaces[self.spaceName].gocdb_sitename:
      tmpGocdbSitename = spaces[self.spaceName].gocdb_sitename
    else:
      tmpGocdbSitename = '.'.join(self.spaceName.split('.')[1:]) if '.' in self.spaceName else self.spaceName

    for machinetypeName in spaces[self.spaceName].machinetypes:
      messageDict = {
                'message_type'          : 'machinetype_status',
                'daemon_version'        : 'Vcycle ' + vcycleVersion + ' vcycled',
                'vacquery_version'      : 'VacQuery ' + vacQueryVersion,
                'cookie'                : cookie,
                'space'                 : self.spaceName,
                'site'                  : tmpGocdbSitename,
                'factory'               : os.uname()[1],
                'num_machinetypes'      : numMachinetypes,
                'time_sent'             : timeNow,

                'machinetype'           : machinetypeName,
                'running_machines'      : spaces[self.spaceName].machinetypes[machinetypeName].runningMachines,
                'running_processors'    : spaces[self.spaceName].machinetypes[machinetypeName].runningProcessors
                     }

      try:
        messageDict['fqan'] = spaces[self.spaceName].machinetypes[machinetypeName].accounting_fqan
      except:
        pass

      if spaces[self.spaceName].machinetypes[machinetypeName].runningHS06 is not None:
        messageDict['running_hs06'] = spaces[self.spaceName].machinetypes[machinetypeName].runningHS06

      messages.append(json.dumps(messageDict))

    return messages

  def updateShutdownTime(self, machine):
    """ If there is a space shutdown time update machines to this value if it
        is closer than their value.
        Return closest shutdown time.
    """
    try:
      shutdowntime_job = int(machine.getFileContents(
        'jobfeatures/shutdowntime_job'))
    except:
      shutdowntime_job = None

    # use space shutdownTime if shutdowntime_job is None
    # or shutdowntime_job has passed
    if self.shutdownTime is not None and \
       (shutdowntime_job is None or \
        shutdowntime_job > self.shutdownTime):
      machine.setFileContents('jobfeatures/shutdowntime_job', str(self.shutdownTime))
      shutdowntime_job = self.shutdownTime

    return shutdowntime_job

  def updateGOCDB(self):

    # Skip if gocdb_cert_file or gocdb_key_file not given
    if self.gocdb_cert_file is None or self.gocdb_key_file is None:
      return
      
    # Skip if we've already sent updates in the last 24 hours
    if os.path.exists('/var/lib/vcycle/gocdb-updated') and \
       time.time() < (os.stat('/var/lib/vcycle/gocdb-updated').st_ctime + 86400):
      return

    file_driver.create_file(
        'gocdb-updated', '',
        stat.S_IWUSR + stat.S_IRUSR + stat.S_IRGRP + stat.S_IROTH)

    voShares = {}
    policyRules = ''
    sharesTotal = 0.0

    for machinetypeName in spaces[self.spaceName].machinetypes:
      if hasattr(spaces[self.spaceName].machinetypes[machinetypeName], 'accounting_fqan'):
        policyRules += 'VOMS:' + spaces[self.spaceName].machinetypes[machinetypeName].accounting_fqan + ','

        try:
          targetShare = float(spaces[self.spaceName].machinetypes[machinetypeName].target_share)
        except:
          targetShare = 0.0

        if targetShare:
          try:
            voName = spaces[self.spaceName].machinetypes[machinetypeName].accounting_fqan.split('/')[1]
          except:
            pass
          else:
            if voName in voShares:
              voShares[voName] += targetShare
            else:
              voShares[voName] = targetShare

            sharesTotal += targetShare

    otherInfo = ''

    for voName in voShares:
      otherInfo += 'Share=%s:%d,' % (voName, int(0.5 + (100 * voShares[voName]) / sharesTotal))

    spaceValues = {
       'ComputingManagerCreationTime':          datetime.datetime.utcnow().replace(microsecond = 0).isoformat() + 'Z',
       'ComputingManagerProductName':           'Vcycle',
       'ComputingManagerProductVersion':        vcycleVersion,
       'ComputingManagerTotalLogicalCPUs':      self.processors_limit
     }

    if otherInfo:
      spaceValues['ComputingManagerOtherInfo'] = otherInfo.strip(',')
     
    if policyRules:
      spaceValues['PolicyRule'] = policyRules.strip(',')
      spaceValues['PolicyScheme'] = 'org.glite.standard'

    vacutils.logLine('Space info for Vcycle space %s in GOCDB site %s: %s' 
                          % (self.spaceName, self.gocdb_sitename, str(spaceValues)))

    try:
      vacutils.updateSpaceInGOCDB(
        self.gocdb_sitename,
        self.spaceName,
        'uk.ac.gridpp.vcycle',
        self.gocdb_cert_file,
        self.gocdb_key_file,
        '/etc/grid-security/certificates',
        'Vcycle ' + vcycleVersion,
        spaceValues,
        None # ONCE GOCDB ALLOWS API CREATION OF ENDPOINTS WE CAN PUT MORE INFO (eg wallclock limits) THERE
             # ONE ENDPOINT OF THE VAC SERVICE PER MACHINETYPE
        )
    except Exception as e:
      vacutils.logLine('Failed to update space info in GOCDB: ' + str(e))
    else:
      vacutils.logLine('Successfully updated space info in GOCDB')

    return

  def sendVacMon(self):

    if not self.vacmons:
      return

    factoryMessage      = self.makeFactoryMessage()
    machinetypeMessages = self.makeMachinetypeMessages()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    for vacmonHostPort in self.vacmons:
      (vacmonHost, vacmonPort) = vacmonHostPort.split(':')

      vacutils.logLine('Sending VacMon status messages to %s:%s' % (vacmonHost, vacmonPort))

      sock.sendto(factoryMessage, (vacmonHost,int(vacmonPort)))

      for machinetypeMessage in machinetypeMessages:
        sock.sendto(machinetypeMessage, (vacmonHost,int(vacmonPort)))

    sock.close()

  def makeMachines(self):

    if self.shutdownTime is not None and self.shutdownTime < time.time():
      vacutils.logLine('Space {} has shutdown time in the past ({}), '\
          'not allocating any more machines'.format(
            self.spaceName, self.shutdownTime))
      return

    vacutils.logLine('Space ' + self.spaceName +
                            ' has ' + str(self.runningProcessors) +
                            ' processor(s) found allocated to running Vcycle VMs out of ' + str(self.totalProcessors) +
                            ' found in any state for any machinetype or none.')

    if self.processors_limit is None:
      vacutils.logLine('The limit for the number of processors which may be allocated is not known to Vcycle.')
    else:
      vacutils.logLine('Vcycle knows the limit on the number of processors is %d, either from its configuration or from the infrastructure.' % self.processors_limit)


    for machinetypeName,machinetype in self.machinetypes.iteritems():
      vacutils.logLine('machinetype ' + machinetypeName +
                              ' has ' + str(machinetype.startingProcessors) +
                              ' starting and ' + str(machinetype.runningProcessors) +
                              ' running processors out of ' + str(machinetype.totalProcessors) +
                              ' found in any state. ' + str(machinetype.notPassedFizzle) +
                              ' not passed fizzle_seconds(' + str(machinetype.fizzle_seconds) +
                              '). ')

    creationsPerCycle  = int(0.9999999 + self.processors_limit * 0.1)
    creationsThisCycle = 0

    # Keep making passes through the machinetypes until limits exhausted
    while True:
      if self.processors_limit is not None and self.totalProcessors >= self.processors_limit:
        vacutils.logLine('Reached limit (%d) on number of processors to allocate for space %s' % (self.processors_limit, self.spaceName))
        return

      if creationsThisCycle >= creationsPerCycle:
        vacutils.logLine('Already reached limit of %d processor allocations this cycle' % creationsThisCycle )
        return

      # For each pass, machinetypes are visited in a random order
      machinetypeNames = self.machinetypes.keys()
      random.shuffle(machinetypeNames)

      # Will record the best machinetype to create
      bestMachinetypeName = None

      for machinetypeName in machinetypeNames:
        if self.machinetypes[machinetypeName].target_share <= 0.0:
          continue

        if self.machinetypes[machinetypeName].processors_limit is not None and self.machinetypes[machinetypeName].totalProcessors >= self.machinetypes[machinetypeName].processors_limit:
          vacutils.logLine('Reached limit (' + str(self.machinetypes[machinetypeName].processors_limit) + ') on number of processors to allocate for machinetype ' + machinetypeName)
          continue

        if self.machinetypes[machinetypeName].max_starting_processors is not None and self.machinetypes[machinetypeName].startingProcessors >= self.machinetypes[machinetypeName].max_starting_processors:
          vacutils.logLine('Reached limit (%d) on processors that can be in starting state for machinetype %s' % (self.machinetypes[machinetypeName].max_starting_processors, machinetypeName))
          continue

        if int(time.time()) < (self.machinetypes[machinetypeName].lastAbortTime + self.machinetypes[machinetypeName].backoff_seconds):
          vacutils.logLine('Free capacity found for %s ... but only %d seconds after last abort'
                                  % (machinetypeName, int(time.time()) - self.machinetypes[machinetypeName].lastAbortTime) )
          continue

        if (int(time.time()) < (self.machinetypes[machinetypeName].lastAbortTime +
                                self.machinetypes[machinetypeName].backoff_seconds +
                                self.machinetypes[machinetypeName].fizzle_seconds)) and \
           (self.machinetypes[machinetypeName].notPassedFizzle > 0):
          vacutils.logLine('Free capacity found for ' +
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
        vacutils.logLine('Free capacity found for ' + bestMachinetypeName + ' within ' + self.spaceName + ' ... creating')

        # This tracks creation attempts, whether successful or not
        creationsThisCycle += self.machinetypes[bestMachinetypeName].min_processors
        self.machinetypes[bestMachinetypeName].startingProcessors += self.machinetypes[bestMachinetypeName].min_processors
        self.machinetypes[bestMachinetypeName].notPassedFizzle += 1

        try:
          self._createMachine(bestMachinetypeName)
        except Exception as e:
          vacutils.logLine('Failed creating machine with machinetype ' + bestMachinetypeName + ' in ' + self.spaceName + ' (' + str(e) + ')')

      else:
        vacutils.logLine('No more free capacity and/or suitable machinetype found within ' + self.spaceName)
        return

  def _createMachine(self, machinetypeName):
    """Generic machine creation"""

    try:
      machineName = self.machinetypes[machinetypeName].makeMachineName()
    except Exception as e:
      vacutils.logLine('Failed construction new machine name (' + str(e) + ')')

    try:
      file_driver.remove_dir('machines/' + machineName)
    except:
      pass

    file_driver.create_dir('machines/' + machineName + '/machinefeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP 
                + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    file_driver.create_dir('machines/' + machineName + '/jobfeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP
                + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    file_driver.create_dir('machines/' + machineName + '/joboutputs',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR +
                stat.S_IWGRP + stat.S_IXGRP + stat.S_IRGRP +
                stat.S_IWOTH + stat.S_IXOTH + stat.S_IROTH)

    file_driver.create_file('machines/' + machineName + '/created', str(int(time.time())), 0600)
    file_driver.create_file('machines/' + machineName + '/updated', str(int(time.time())), 0600)
    file_driver.create_file('machines/' + machineName + '/machinetype_name', machinetypeName,  0644)
    file_driver.create_file('machines/' + machineName + '/space_name',  self.spaceName, 0644)

    if self.zones:
      zone = random.choice(self.zones)
      file_driver.create_file('machines/' + machineName + '/zone',  zone,   0644)
    else:
      zone = None

    if self.machinetypes[machinetypeName].remote_joboutputs_url:
      joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
    else:
      joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'

    if self.machinetypes[machinetypeName].root_image and (self.machinetypes[machinetypeName].root_image.startswith('http://') or self.machinetypes[machinetypeName].root_image.startswith('https://')):
      rootImageURL = self.machinetypes[machinetypeName].root_image
    else:
      rootImageURL = None    
      
    userDataOptions = self.machinetypes[machinetypeName].options.copy()
    
    if self.machinetypes[machinetypeName].cvmfsProxyMachinetype:
      # If we define a cvmfs_proxy_machinetype, then use the IPs of heartbeat producing
      # machines of that machinetype to create the user_data_option_cvmfs_proxy
      # Any existing value for that option is appended to the list, using the semicolon syntax
      
      if self.machinetypes[machinetypeName].cvmfsProxyMachinetype not in self.machinetypes:
        raise VcycleError('Machinetype %s (cvmfs_proxy_machinetype) does not exist!'
                               % self.machinetypes[machinetypeName].cvmfsProxyMachinetype)
                               
      ipList = []
      for heartbeatMachineName in self.machinetypes[self.machinetypes[machinetypeName].cvmfsProxyMachinetype].heartbeatMachines:
        ipList.append('http://%s:%d' % (self.machines[heartbeatMachineName].ip, self.machinetypes[machinetypeName].cvmfsProxyMachinetypePort))

      if ipList:
        # We only change any existing value if we found machines of cvmfs_proxy_machinetype
        if 'user_data_option_cvmfs_proxy' not in userDataOptions:
          existingProxyOption = ''
        else:
          existingProxyOption = ';' + userDataOptions['user_data_option_cvmfs_proxy']

        userDataOptions['user_data_option_cvmfs_proxy'] = '|'.join(ipList) + existingProxyOption
      else:
        vacutils.logLine('No machines found in machinetype %s (cvmfs_proxy_machinetype) - using defaults'
                                 % self.machinetypes[machinetypeName].cvmfsProxyMachinetype)
        
    try:
      userDataContents = vacutils.createUserData(
          shutdownTime         = int(time.time() +
            self.machinetypes[machinetypeName].max_wallclock_seconds),
          machinetypePath      = self.machinetypes[machinetypeName].machinetype_path,
          options              = userDataOptions,
          versionString        = 'Vcycle ' + vcycleVersion,
          spaceName            = self.spaceName,
          machinetypeName      = machinetypeName,
          userDataPath         = self.machinetypes[machinetypeName].user_data,
          rootImageURL         = rootImageURL,
          hostName             = machineName,
          uuidStr              = None,
          machinefeaturesURL   = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures',
          jobfeaturesURL       = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures',
          joboutputsURL        = joboutputsURL,
          heartbeatMachinesURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/spaces/' + self.spaceName + '/heartbeatmachines')
    except Exception as e:
      raise VcycleError('Failed getting user_data file (' + str(e) + ')')

    try:
      file_driver.create_file('machines/' + machineName + '/user_data',
                              userDataContents, 0600)
    except:
      raise VcycleError('Failed to writing /var/lib/vcycle/machines/' + machineName + '/user_data')

    # Call the API-specific method to actually create the machine
    try:
      self.createMachine(machineName, machinetypeName, zone)
    except Exception as e:
      vacutils.logLine('Creation of machine %s fails with: %s' % (machineName, str(e)))
    
    # MJF. Some values may be set by self.createMachine() from the API!

    # $MACHINEFEATURES first

    # We maintain the fiction that this is a single-VM hypervisor, as we don't know the hypervisor details
    file_driver.create_file('machines/' + machineName + '/machinefeatures/jobslots',
                               "1", 0644)

    file_driver.create_file('machines/' + machineName + '/machinefeatures/total_cpu',
                               str(self.machines[machineName].processors), 0644)

    # phys_cores and log_cores keys are deprecated
    file_driver.create_file('machines/' + machineName + '/machinefeatures/phys_cores',
                               str(self.machines[machineName].processors), 0644)
    file_driver.create_file('machines/' + machineName + '/machinefeatures/log_cores',
                               str(self.machines[machineName].processors), 0644)

    if self.machinetypes[machinetypeName].hs06_per_processor:
      file_driver.create_file('machines/' + machineName + '/machinefeatures/hs06',
                                 str(self.machinetypes[machinetypeName].hs06_per_processor * self.machines[machineName].processors),
                                 0644)

    file_driver.create_file('machines/' + machineName + '/machinefeatures/shutdown_time',
                               str(int(time.time()) + self.machinetypes[machinetypeName].max_wallclock_seconds), 0644)

    # Then $JOBFEATURES

    # check for existence of shutdownTime and whether wallclock limit is closer
    if (self.shutdownTime is None or
        int(time.time()) + self.machinetypes[machinetypeName].maxWallclockSeconds < self.shutdownTime):
      file_driver.create_file('machines/' + machineName + '/jobfeatures/shutdowntime_job',
                                str(int(time.time()) + self.machinetypes[machinetypeName].max_wallclock_seconds), 0644)
    else:
      file_driver.create_file('machines/' + machineName + '/jobfeatures/shutdowntime_job',
                                str(self.shutdownTime), 0644)

    file_driver.create_file('machines/' + machineName + '/jobfeatures/wall_limit_secs',
                               str(self.machinetypes[machinetypeName].max_wallclock_seconds), 0644)

    # We assume worst case that CPU usage is limited by wallclock limit
    file_driver.create_file('machines/' + machineName + '/jobfeatures/cpu_limit_secs',
                               str(self.machinetypes[machinetypeName].max_wallclock_seconds), 0644)

    # Calculate MB for this VM ("job")
    file_driver.create_file('machines/' + machineName + '/jobfeatures/max_rss_bytes',
                               str(self.machinetypes[machinetypeName].rss_bytes_per_processor *
                                   self.machines[machineName].processors), 0644)

    # All the cpus are allocated to this one VM ("job")
    file_driver.create_file('machines/' + machineName + '/jobfeatures/allocated_cpu',
                               str(self.machines[machineName].processors), 0644)
    # allocated_CPU key name is deprecated
    file_driver.create_file('machines/' + machineName + '/jobfeatures/allocated_CPU',
                               str(self.machines[machineName].processors), 0644)


    file_driver.create_file('machines/' + machineName + '/jobfeatures/jobstart_secs',
                               str(int(time.time())), 0644)

    if self.machines[machineName].uuidStr is not None:
      file_driver.create_file('machines/' + machineName + '/jobfeatures/job_id',
                                 self.machines[machineName].uuidStr, 0644)

    if self.machinetypes[machinetypeName].hs06_per_processor:
      file_driver.create_file('machines/' + machineName + '/jobfeatures/hs06_job',
                                 str(self.machinetypes[machinetypeName].hs06_per_processor *
                                     self.machines[machineName].processors), 0644)

    # We do not know max_swap_bytes, scratch_limit_bytes etc so ignore them

  def updateMachineTotals(self):
    """ Run through the machines and update various totals """

    for machineName, machine in self.machines.iteritems():

      # Update overall spaces totals for newly created machine
      self.totalMachines += 1
      self.totalProcessors += machine.processors

      if machine.state == MachineState.running:
        self.runningMachines += 1
        self.runningProcessors += machine.processors

        if machine.hs06:
          if self.runningHS06:
            self.runningHS06 += machine.hs06

      # update machinetypes totals
      if machine.machinetypeName in self.machinetypes:
        machinetype = self.machinetypes[machine.machinetypeName]
        machinetype.totalMachines += 1
        machinetype.totalProcessors += machine.processors

        if machinetype.target_share > 0.0:
          hs06Weight = machine.hs06 if machine.hs06 else float(machine.processors)
          machinetype.weightedMachines += hs06Weight / machinetype.target_share

        if machine.state == MachineState.starting:
          machinetype.startingProcessors += machine.processors

        if machine.state == MachineState.running:
          machinetype.runningMachines += 1
          machinetype.runningProcessors += machine.processors
          if machine.hs06:
            machinetype.runningHS06 += machine.hs06

        if (machine.state == MachineState.starting
            or (machine.state == MachineState.running
              and (int(time.time()) - machine.startedTime)
              < machinetype.fizzle_seconds)):
          machinetype.notPassedFizzle += 1

  def oneCycle(self):

    try:
      self.connect()
    except Exception as e:
      vacutils.logLine('Skipping ' + self.spaceName + ' this cycle: ' + str(e))
      return

    try:
      self.scanMachines()
      self.updateMachineTotals()
    except Exception as e:
      vacutils.logLine('Giving up on ' + self.spaceName + ' this cycle: ' + str(e))
      return

    try:
      self.sendVacMon()
    except Exception as e:
      vacutils.logLine('Sending VacMon messages fails: ' + str(e))

    try:
      # The update is only every hour as there is a heartbeat file
      self.updateGOCDB()
    except Exception as e:
      vacutils.logLine('Updating GOCDB fails: ' + str(e))

    try:
      self.deleteMachines()
    except Exception as e:
      vacutils.logLine('Deleting old machines in ' + self.spaceName + ' fails: ' + str(e))
      # We carry on because this isn't fatal
      
    try:
       self.createHeartbeatMachines()
    except Exception as e:
      vacutils.logLine('Creating heartbeat machine lists for ' + self.spaceName + ' fails: ' + str(e))
      
    try:
      self.makeMachines()
    except Exception as e:
      vacutils.logLine('Making machines in ' + self.spaceName + ' fails: ' + str(e))

def readConf(printConf = False, updatePipes = True, parser = None):

  global vcycleVersion, spaces

  try:
    f = open('/var/lib/vcycle/VERSION', 'r')
    vcycleVersion = f.readline().split('=',1)[1].strip()
    f.close()
  except:
    vcycleVersion = '0.0.0'

  spaces = {}

  if parser == None:
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
            vacutils.logLine('Failed to parse /etc/vcycle.d/' + oneFile + ' (' + str(e) + ')')

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
        api = parser.get(spaceSectionName, 'api')
      except:
        raise VcycleError('api missing from [space ' + spaceName + ']')

      if string.translate(api, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
        raise VcycleError('Name of api in [space ' + spaceName + '] can only contain a-z 0-9 or _')

      try:
        apiVersion = parser.get(spaceSectionName, 'api_version')
      except:
        apiVersion = None
      else:
        if string.translate(apiVersion, None, '0123456789abcdefghijklmnopqrstuvwxyz._-') != '':
          raise VcycleError('Name of api_version in [space ' + spaceName + '] can only contain a-z 0-9 - . or _')

      for subClass in BaseSpace.__subclasses__():
        if subClass.__name__ == api.capitalize() + 'Space':
          try:
            spaces[spaceName] = subClass(api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)
          except Exception as e:
            raise VcycleError('Failed to initialise space ' + spaceName + ' (' + str(e) + ')')
          else:
            break

      if spaceName not in spaces:
        raise VcycleError(api + ' is not a supported API for managing spaces')

    elif sectionType != 'machinetype' and sectionType != 'vacuum_pipe':
      raise VcycleError('Section type ' + sectionType + 'not recognised')

  # else: Skip over vacuum_pipe and machinetype sections, which are parsed during the space class initialization

  if printConf:
    print 'Configuration including any machinetypes from Vacuum Pipes:'
    print
    parser.write(sys.stdout)
    print

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
        vacutils.logLine('Saving joboutputs to /var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName)
        logJoboutputs(spaceName, machinetypeName, machineName)

      # Always delete the working copies
      try:
        file_driver.remove_dir('machines/' + machineName)
      except:
        vacutils.logLine('Failed deleting /var/lib/vcycle/machines/' + machineName)

def logJoboutputs(spaceName, machinetypeName, machineName):

  if os.path.exists('/var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName):
    # Copy (presumably) already exists so don't need to do anything
    return

  try:
    file_driver.create_dir(
        'joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName,
        stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP
        + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
  except:
    vacutils.logLine('Failed creating /var/lib/vcycle/joboutputs/' + spaceName + '/' + machinetypeName + '/' + machineName)
    return

  try:
    # Get the list of files that the VM wrote in its /etc/joboutputs
    outputs = os.listdir('/var/lib/vcycle/machines/' + machineName + '/joboutputs')
  except:
    vacutils.logLine('Failed reading /var/lib/vcycle/machines/' + machineName + '/joboutputs')
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
          vacutils.logLine('Failed copying /var/lib/vcycle/machines/' + machineName + '/joboutputs/' + oneOutput +
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
            file_driver.remove_dir('joboutputs/' + spaceDir + '/' + machinetypeDir + '/' + hostNameDir)
            vacutils.logLine('Deleted /var/lib/vcycle/joboutputs/' + spaceDir + '/' + machinetypeDir +
                                    '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')
          except:
            vacutils.logLine('Failed deleting /var/lib/vcycle/joboutputs/' + spaceDir + '/' +
                                    machinetypeDir + '/' + hostNameDir + ' (' + str((int(time.time()) - hostNameDirCtime)/86400.0) + ' days)')

def getSpaces():
  global spaces
  return spaces

### END ###
