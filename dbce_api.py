#!/usr/bin/python
#
#  dbce_api.py - a DBCE plugin for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Luis Villazon Esteban, CERN.
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
#            Luis.Villazon.Esteban@cern.ch
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

import vcycle.vacutils


class DbceError(Exception):
  pass

class DbceSpace(vcycle.BaseSpace):

  def __init__(self, api, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, spaceName, parser, spaceSectionName)

    # OpenStack-specific initialization
    try:
      self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
    except Exception as e:
      raise DbceError('tenancy_name is required in DBCE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.url = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise DbceError('url is required in DBCE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.key = parser.get(spaceSectionName, 'key')
    except Exception as e:
      raise DbceError('key is required in DBCE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.version = parser.get(spaceSectionName, 'version')
    except Exception as e:
      raise DbceError('version is required in DBCE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.network = parser.get(spaceSectionName, 'network')
    except Exception as e:
      raise DbceError('network is required in DBCE [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
    # Connect to the DBCE service
    #Nothing to do
    pass

  def scanMachines(self):
    import time
    """Query DBCE compute service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for
    # either (a) ignorning non-Vcycle VMs but updating self.totalMachines
    # or (b) creating a Machine object for the VM in self.spaces

    try:
      result = self.httpRequest("%s/%s/machines" % (self.url, self.version),
                                headers = ['DBCE-ApiKey:'+ self.key])
    except Exception as e:
      raise DbceError('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    for oneServer in result['response']['data']:

      # Just in case other VMs are in this space
      if oneServer['name'][:7] != 'vcycle-':
        # Still count VMs that we didn't create and won't manage, to avoid going above space limit
        self.totalMachines += 1
        continue

      # checks if the machine belongs to the space name
      # if the machine does not belong to the actual space name,
      # the machine will be omitted.
      space_file = "/var/lib/vcycle/machines/%s/space_name" % oneServer['name']
      if os.path.isfile(space_file):
        server_space_name = open(space_file).read()
        if server_space_name != self.spaceName:
            continue
      else:
          continue

      uuidStr = str(oneServer['id'])
      ip = '0.0.0.0'
      if os.path.isfile("/var/lib/vcycle/machines/%s/started" % oneServer['name']):
          createdTime = int(open("/var/lib/vcycle/machines/%s/started" % oneServer['name']).read())
          updatedTime = createdTime
          startedTime = createdTime
      else:
          createdTime  = int(time.time())
          updatedTime  = int(time.time())
          startedTime = int(time.time())

      status     = str(oneServer['state'])
      machinetypeName = oneServer['name'][oneServer['name'].find('-')+1:oneServer['name'].rfind('-')]

      if status == 'started':
          state = vcycle.MachineState.running
      elif status == 'error':
          state = vcycle.MachineState.failed
      elif status == 'stopped':
        try:
            if os.path.isfile("/var/lib/vcycle/machines/%s/started" % oneServer['name']):
                if int(time.time()) - createdTime < self.machinetypes[machinetypeName].fizzle_seconds:
                    state = vcycle.MachineState.starting
                else:
                    state = vcycle.MachineState.shutdown
            else:
                state = vcycle.MachineState.unknown
        except Exception as e:
            state = vcycle.MachineState.unknown
      else:
          state = vcycle.MachineState.failed

      self.machines[oneServer['name']] = vcycle.Machine(name        = oneServer['name'],
                                                               spaceName   = self.spaceName,
                                                               state       = state,
                                                               ip          = ip,
                                                               createdTime = createdTime,
                                                               startedTime = startedTime,
                                                               updatedTime = updatedTime,
                                                               uuidStr     = uuidStr,
                                                               machinetypeName  = machinetypeName)


  def createMachine(self, machineName, machinetypeName):

    # DBCE-specific machine creation steps

    try:
        request = {
            'name': machineName,
            'platform': {
                'id': self.tenancy_name
            },
            'image': {
                'id': self.machinetypes[machinetypeName].root_image
            },
            'configuration': {
                'id': self.machinetypes[machinetypeName].flavor_name,
            },
            'network': {
                'id': self.network,
            },
            'cloudConfig': base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read())
        }


    except Exception as e:
      raise DbceError('Failed to create new machine: ' + str(e))

    try:
      result = self.httpRequest("%s/%s/machines" % (self.url, self.version),
                             request,
                             verbose=False,
                             headers = ['DBCE-ApiKey: '+ self.key])
    except Exception as e:
      raise DbceError('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + str(result['response']['data']['id']) + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = None,
                                                       machinetypeName  = machinetypeName)

  def deleteOneMachine(self, machineName):

    try:
      self.httpRequest("%s/%s/machines/%s" % (self.url, self.version, self.machines[machineName].uuidStr),
                    request =  None,
                    method  = 'DELETE',
                    headers = ['Accept: application/json',
                               'Content-Type: application/json',
                               'DBCE-ApiKey: '+ self.key])
    except Exception as e:
      raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' via ' + self.url + ' (' + str(e) + ')')
