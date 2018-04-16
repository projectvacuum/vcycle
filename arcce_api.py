#!/usr/bin/python
#
#  arcce_api.py - an ARC CE plugin for Vcycle
#
#  Andrew McNab, University of Manchester.
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
import time
import json
import shutil
import string
import urllib
import random
import base64
import StringIO
import tempfile
import calendar
import subprocess

import vcycle.vacutils

class ArcceError(Exception):
  pass

class ArcceSpace(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    self.maxStartingSeconds = None # no limit

    try:
      self.url = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise ArcceError('url is required in ARC CE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.queue = parser.get(spaceSectionName, 'queue')
    except Exception as e:
      raise ArcceError('queue is required in ARC CE [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
    pass

  def scanMachines(self):
    """Query ARC CE compute service for details of machines in this space"""

    # For each job found in the space, this method is responsible for
    # either (a) ignorning non-Vcycle jobs but updating self.totalProcessors
    # or (b) creating a Machine object for the job in self.spaces
    
    allJobIDs = {}
    recentJobIDs = []
    now = time.time()
    fd,path = tempfile.mkstemp()

    for jobidEncoded in os.listdir('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/'):
  
      jobID = urllib.unquote(jobidEncoded)
          
      with open('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/' + jobidEncoded) as f:
        machineName = f.read().strip()
        
      allJobIDs[jobID] = machineName

      if os.stat('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/' + jobidEncoded).st_ctime > now - 3600:
        recentJobIDs.append(jobID)
    
      try:
        os.write(fd, jobID + '\n')
      except Exception as e:
        print str(e)

    os.close(fd)

    with subprocess.Popen('X509_USER_PROXY=%s arcstat --jobids-from-file=%s' % 
                          ('/var/lib/vcycle/spaces/' + self.spaceName + '/x509proxy.pem', path),
                          shell=True, stdout=subprocess.PIPE).stdout as p:
      rawStatuses = p.read()

    os.remove(path)

    allJobStatuses = self.parseArcceJobStatus(rawStatuses)
    
    for jobID in allJobIDs:
    
      machineName = allJobIDs[jobID]

      # Collect values saved in machine's directory
      try:
        machinetypeName = open('/var/lib/vcycle/machines/%s/machinetype_name' % machineName, 'r').readline().strip()
      except:
        vcycle.vacutils.logLine('Failed to read machinetype_name file for %s!' % machineName)
        continue

      if jobID in allJobStatuses:
        # Map ARC CE Status to Vcycle state
        if allJobStatuses[jobID]['State'] in ('Accepted','Preparing','Submitting','Queuing'):
          state = vcycle.MachineState.starting
        elif allJobStatuses[jobID]['State'] in ('Running','Finishing'):
          state = vcycle.MachineState.running
        elif allJobStatuses[jobID]['State'] in ('Failed','Hold','Deleted','Killed'):
          state = vcycle.MachineState.failed
        elif allJobStatuses[jobID]['State'] in ('Finished','Other'):
          state = vcycle.MachineState.shutdown
        else:
          state = vcycle.MachineState.unknown

      elif jobID in recentJobIDs:
        # If not yet in the arcstat output then starting
        state = vcycle.MachineState.starting
        
      else:
        # Do not understand what is going on
        state = vcycle.MachineState.unknown

      self.machines[machineName] = vcycle.shared.Machine(name             = machineName,
                                                         spaceName        = self.spaceName,
                                                         state            = state,
                                                         ip               = '0.0.0.0',
                                                         createdTime      = None,
                                                         startedTime      = None,
                                                         updatedTime      = None,
                                                         uuidStr          = jobID,
                                                         machinetypeName  = machinetypeName,
                                                         zone             = None)

  def parseArcceJobStatus(self, rawStatuses):
    # State machine to go through rawStatuses from arcstat
    # output, populating jobs list with status information

    jobs  = {}
    job   = {}
    jobID = None

    for line in rawStatuses.split('\n'):

      if line.startswith('Job: '):
        jobID = line.split()[1] 
        job   = {}

      elif line.strip().startswith('State: '):
        job['State'] = line.split()[1]

      elif line.strip().startswith('Exit Code: '):
        job['ExitCode'] = line.split()[2]

      elif jobID and line.strip() == '':
        if 'State' in job:
          # Add properly formed job items to the jobs list
          jobs[jobID] = job

        job   = {}
        jobID = None

    return jobs

  def createMachine(self, machineName, machinetypeName, zone = None):
    # ARC CE-specific job submission

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/rsl',
                               '''&( executable = "user_data" )
( stdout = "stdout" )
( stderr = "stderr" )
( inputfiles = ( "user_data" "/var/lib/vcycle/machines/''' + machineName + '''/user_data" ) )
( outputfiles = ( "stdout" "" ) ( "stderr" "" ) )
( queue = "''' + self.queue + '''" )
( jobname = "''' + machineName + '''" )
''',
                               0600, '/var/lib/vcycle/tmp')

    try:
     subprocess.call('X509_USER_PROXY=%s arcsub --direct --cluster=%s --jobids-to-file=%s %s' %
                        ('/var/lib/vcycle/spaces/' + self.spaceName + '/x509proxy.pem',
                         self.url, 
                         '/var/lib/vcycle/machines/' + machineName + '/jobid',
                         '/var/lib/vcycle/machines/' + machineName + '/rsl'),
                     shell=True)

    except Exception as e:
      raise ArcceError('Failed to submit new job %s: %s' % (machineName, str(e)))

    try:
      jobID = open('/var/lib/vcycle/machines/' + machineName + '/jobid', 'r').read().strip()
    except:
      raise ArcceError('Could not get Job ID saved by %s job submission' % machineName)

    vcycle.vacutils.createFile('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/' + urllib.quote(jobID,''), machineName, 0600, '/var/lib/vcycle/tmp')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + jobID + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = jobID,
                                                       machinetypeName  = machinetypeName)

  def deleteOneMachine(self, machineName):

    try:
      jobID = open('/var/lib/vcycle/machines/' + machineName + '/jobid', 'r').read().strip()

      # All we do is remove it from the list of jobids to ignore it from now on
      os.remove('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/' + urllib.quote(jobID, ''))
    except Exception as e:
      vcycle.vacutils.logLine('Failed deleting %s (%s)' % (machineName, str(e)))
