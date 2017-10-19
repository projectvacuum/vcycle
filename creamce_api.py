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
import pycurl
import random
import base64
import StringIO
import tempfile
import calendar

import vcycle.vacutils

class CreamceError(Exception):
  pass

class CreamceSpace(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName)

    # CREAM CE specific initialization
    try:
      self.ce_host_port_queue = parser.get(spaceSectionName, 'ce_host_port_queue')
    except Exception as e:
      raise CreamceError('ce_host_port_queue is required in Cream CE [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.usercert = parser.get(spaceSectionName, 'usercert')
    except Exception as e:
      self.usercert = None
      
    try:
      self.userkey = parser.get(spaceSectionName, 'userkey')
    except Exception as e:
      self.userkey = None
      
    if self.usercert and not self.userkey:
      self.userkey = self.usercert
    elif self.userkey and not self.usercert:
      self.usercert = self.userkey

    if not self.username and not self.usercert:      
      raise CreamceError('X.509 usercert/userkey is required in Cream CE [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
  # Wrapper around the connect methods and some common post-connection updates

    # Try to get the limit on the number of processors in this project
    processorsLimit =  self._getProcessorsLimit()

    # Try to use it for this space
    if self.max_processors is None:
      vcycle.vacutils.logLine('No limit on processors set in Vcycle configuration')
      if processorsLimit is not None:
        vcycle.vacutils.logLine('Processors limit set to %d from Cream CE' % processorsLimit)
        self.max_processors = processorsLimit
    else:
      vcycle.vacutils.logLine('Processors limit set to %d in Vcycle configuration' % self.max_processors)
     
  def scanMachines(self):
    """Query Cream CE compute service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for 
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces

    try:
      result = self.httpRequest(self.computeURL + '/servers/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise CreamceError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneServer in result['response']['servers']:
    
      try:
        machineName = str(oneServer['metadata']['name'])
      except:
        machineName = oneServer['name']

      try:
        flavorID = oneServer['flavor']['id']
      except:
        flavorID   = None
        processors = 1
      else:
        try:
          processors = self.flavors[flavorID]['processors']
        except:
          processors = 1
       
      # Just in case other VMs are in this space
      if machineName[:7] != 'vcycle-':
        # Still count VMs that we didn't create and won't manage, to avoid going above space limit
        self.totalProcessors += processors
        continue

      uuidStr = str(oneServer['id'])

      # Try to get the IP address. Always use the zeroth member of the earliest network
      try:
        ip = str(oneServer['addresses'][ min(oneServer['addresses']) ][0]['addr'])
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

      try:
        machinetypeName = str(oneServer['metadata']['machinetype'])
      except:
        machinetypeName = None

      try:
        zone = str(oneServer['OS-EXT-AZ:availability_zone'])
      except:
        zone = None

      if taskState == 'Deleting':
        state = vcycle.MachineState.deleting
      elif status == 'ACTIVE' and powerState == 1:
        state = vcycle.MachineState.running
      elif status == 'BUILD' or status == 'ACTIVE':
        state = vcycle.MachineState.starting
      elif status == 'SHUTOFF':
        state = vcycle.MachineState.shutdown
      elif status == 'ERROR':
        state = vcycle.MachineState.failed
      elif status == 'DELETED':
        state = vcycle.MachineState.deleting
      else:
        state = vcycle.MachineState.unknown

      self.machines[machineName] = vcycle.shared.Machine(name             = machineName,
                                                         spaceName        = self.spaceName,
                                                         state            = state,
                                                         ip               = ip,
                                                         createdTime      = createdTime,
                                                         startedTime      = startedTime,
                                                         updatedTime      = updatedTime,
                                                         uuidStr          = uuidStr,
                                                         machinetypeName  = machinetypeName,
                                                         zone             = zone)

  def createMachine(self, machineName, machinetypeName, zone = None):

    # Cream CE-specific machine creation steps

    try:
      if self.machinetypes[machinetypeName].remote_joboutputs_url:
        joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
      else:
        joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'
    
      request = { 'server' : 
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(machinetypeName),
                    'flavorRef' : self.getFlavorID(self.machinetypes[machinetypeName].flavor_name),
                    'metadata'  : { 'cern-services'   : 'false',
                                    'name'	      : machineName,
                                    'machinetype'     : machinetypeName,
                                    'machinefeatures' : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures',
                                    'jobfeatures'     : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures',
                                    'machineoutputs'  : joboutputsURL,
                                    'joboutputs'      : joboutputsURL  }
                    # Changing over from machineoutputs to joboutputs, so we set both in the metadata for now, 
                    # but point them both to the joboutputs directory that we now provide
                  }    
                }

      if self.network_uuid:
        request['server']['networks'] = [{"uuid": self.network_uuid}]
        vcycle.vacutils.logLine('Will use network %s for %s' % (self.network_uuid, machineName))

      if zone:
        request['server']['availability_zone'] = zone
        vcycle.vacutils.logLine('Will request %s be created in zone %s of space %s' % (machineName, zone, self.spaceName))

      if self.machinetypes[machinetypeName].root_public_key:
        request['server']['key_name'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise CreamceError('Failed to create new machine %s: %s' % (machineName, str(e)))

    try:
      result = self.httpRequest(self.computeURL + '/servers',
                                jsonRequest = request,
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise CreamceError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    try:
      uuidStr = str(result['response']['server']['id'])
    except:
      raise CreamceError('Could not get VM UUID from VM creation response (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + uuidStr + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = uuidStr,
                                                       machinetypeName  = machinetypeName)

  def deleteOneMachine(self, machineName):

    try:
      self.httpRequest(self.computeURL + '/servers/' + self.machines[machineName].uuidStr,
                       method = 'DELETE',
                       headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')
