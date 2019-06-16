#!/usr/bin/python
#
#  creamce_api.py - an CREAM CE plugin for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-7. All rights reserved.
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

class CreamceError(Exception):
  pass

class CreamceSpace(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    self.maxStartingSeconds = None # no limit

    try:
      self.url = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise CreamceError('url is required in CREAM CE [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
    pass

  def scanMachines(self):
    """Query Cream CE compute service for details of machines in this space"""

    # For each job found in the space, this method is responsible for
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces

    fd,path = tempfile.mkstemp()

    os.write(fd, '##CREAMJOBS##\n')

    for jobidEncoded in os.listdir('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/'):
      try:
        os.write(fd, urllib.unquote(jobidEncoded) + '\n')
      except Exception as e:
        print str(e)

    os.close(fd)

    with subprocess.Popen('glite-ce-job-status --donot-verify-ac-sign --level 0 --input %s' % path, shell=True, stdout=subprocess.PIPE).stdout as p:
      rawStatuses = p.read()

    os.remove(path)

    # Convert machines from None to an empty dictionary since we successfully connected
    self.machines = {}

    for oneStatus in self.parseGliteCeJobStatus(rawStatuses):

      try:
        with open('/var/lib/vcycle/spaces/' + self.spaceName + '/jobids/' + urllib.quote(oneStatus['JobID'], '')) as f:
          machineName = f.read().strip()
      except:
        vcycle.vacutils.logLine('Failed to read jobid file for %s!' % oneStatus['JobID'])
        continue

      # Collect values saved in machine's directory

      try:
        machinetypeName = open('/var/lib/vcycle/machines/%s/machinetype_name' % machineName, 'r').readline().strip()
      except:
        vcycle.vacutils.logLine('Failed to read machinetype_name file for %s!' % machineName)
        continue

      # Map CREAM Status to Vcycle state
      if oneStatus['Status'] in ('REGISTERED','PENDING','IDLE'):
        state = vcycle.MachineState.starting
      elif oneStatus['Status'] in ('RUNNING','REALLY-RUNNING'):
        state = vcycle.MachineState.running
      elif oneStatus['Status'] in ('DONE-FAILED','ABORTED'):
        state = vcycle.MachineState.failed
      elif oneStatus['Status'] in ('DONE-OK','CANCELLED'):
        state = vcycle.MachineState.shutdown
      else:
        state = vcycle.MachineState.unknown

      self.machines[machineName] = vcycle.shared.Machine(name             = machineName,
                                                         spaceName        = self.spaceName,
                                                         state            = state,
                                                         ip               = '0.0.0.0',
                                                         createdTime      = None,
                                                         startedTime      = None,
                                                         updatedTime      = None,
                                                         uuidStr          = oneStatus['JobID'],
                                                         machinetypeName  = machinetypeName,
                                                         zone             = None)

  def parseGliteCeJobStatus(self, rawStatuses):
    # State machine to go through rawStatuses from glite-ce-job-status
    # output, populating jobs list with status information

    jobs = []
    job  = None

    for line in rawStatuses.split('\n'):

      if line.startswith('******  JobID=['):
        job = { 'JobID': line.split('[')[1].split(']')[0] }

      elif line.strip().startswith('Status        = ['):
        job['Status'] = line.split('[')[1].split(']')[0]

      elif line.strip().startswith('ExitCode      = ['):
        job['ExitCode'] = line.split('[')[1].split(']')[0]

      elif job and line.strip() == '':
        if 'JobID' in job and 'Status' in job:
          # Add properly formed job items to the jobs list
          jobs.append(job)

        job = None

    return jobs

  def createMachine(self, machineName, machinetypeName, zone = None):
    # Cream CE-specific job submission


    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jdl',
                               '''[
Type = "Job";
JobType = "Normal";
Executable = "user_data";
StdOutput = "stdout.log";
StdError = "stderr.log";
InputSandbox = {"/var/lib/vcycle/machines/''' + machineName + '''/user_data"};
OutputSandbox = {"stdout.log", "stderr.log"};
OutputSandboxBaseDestURI = "gsiftp://localhost";
]
''',
                               0600, '/var/lib/vcycle/tmp')

    if self.url.startswith('https://'):
      endpoint = self.url[8:]
    else:
      endpoint = self.url

    try:
      with subprocess.Popen('glite-ce-job-submit --autm-delegation --donot-verify-ac-sign --resource %s %s' %
                          (endpoint, '/var/lib/vcycle/machines/' + machineName + '/jdl'),
                          shell=True, stdout=subprocess.PIPE).stdout as p:
        jobID = p.read().strip()


    except Exception as e:
      raise CreamceError('Failed to submit new job %s: %s' % (machineName, str(e)))


    if not jobID:
      raise CreamceError('Could not get Job ID from %s job submission response' % machineName)

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobid', jobID, 0600, '/var/lib/vcycle/tmp')

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
