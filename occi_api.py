#!/usr/bin/python
#
#  occi_api.py - common functions, classes, and variables for Vcycle
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

import vcycle.vacutils

class OcciError(Exception):
  pass

class OcciSpace(vcycle.BaseSpace):

  def __init__(self, api, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, spaceName, parser, spaceSectionName)

    # OCCI-specific initialization

    try:
      self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
    except Exception as e:
      raise OcciError('tenancy_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.queryURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise OcciError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    if self.queryURL.endswith('/'):
      self.queryURL = self.queryURL[:-1]

    try:
      self.username = parser.get(spaceSectionName, 'username')
    except Exception as e:
      raise OcciError('username is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      # We use ROT-1 (A -> B etc) encoding so browsing around casually doesn't
      # reveal passwords in a memorable way. 
      self.password = ''.join([ chr(ord(c)-1) for c in parser.get(spaceSectionName, 'password')])
    except Exception as e:
      raise OcciError('password is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
  # Connect to the OCCI service
  
    try:
      result = self.httpRequest(self.queryURL + '/-/', method = 'HEAD', anyStatus = True)
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.queryURL + ' (' + str(e) + ')')

    # This is implicitly only for Keystone authentication
    if result['status'] != 401 or \
       result['headers'] is None or \
       'www-authenticate' not in result['headers']:
      raise OcciError('Do not recognise response when connecting to ' + self.queryURL + ' (' + str(e) + ')')

    # Explicitly check for Keystone using hard-coded string index values for now
    if not result['headers']['www-authenticate'][0].startswith("Keystone uri="):
      raise OcciError('Only Keystone authentication is currently supported (instead got "' + result['headers']['www-authenticate'][0] + '")')

    try:
      keystoneURL = result['headers']['www-authenticate'][0][14:-1]
    except:
      raise OcciError('Failed to find Keystone URL in ' + result['headers']['www-authenticate'][0])

    vcycle.vacutils.logLine('Found Keystone URL ' + keystoneURL)

    # Now try to get the token from Keystone itself
 
    try:
      result = self.httpRequest(keystoneURL + 'v2.0/tokens',
                               { 'auth' : { 
                                            'tenantName'          : self.tenancy_name,
                                            'passwordCredentials' : { 'username' : self.username, 
                                                                      'password' : self.password 
                                                                    }
                                          }
                               } )
    except Exception as e:
      raise OcciError('Cannot connect to ' + keystoneURL + ' (' + str(e) + ')')

    self.token = str(result['response']['access']['token']['id'])

    # Now go back to Query Interface to get the services
    
    try:
      result = self.httpRequest(self.queryURL + '/-/',
                             headers = [ 'X-Auth-Token: ' + self.token,
                                         'User-Agent: Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type: text/occi',
                                         'Accept: text/occi',
                                       ])
    except Exception as e:
      raise OcciError('Cannot reconnect to ' + self.queryURL + ' (' + str(e) + ')')

    pprint.pprint(result)
                                                       
    self.computeURL = None

    for x in vcycle.vacutils.splitCommaHeaders(result['headers']['category']):
      if x.startswith('compute;'):
        try:
          self.computeURL = re.compile('location="([^"]*)"').findall(x)[0]
        except:
          pass
        else:
          vcycle.vacutils.logLine('computeURL = ' + self.computeURL)
    
    if not self.computeURL:
      raise OcciError('No compute service URL found from ' + self.identityURL)

    vcycle.vacutils.logLine('Connected to ' + self.queryURL + ' for space ' + self.spaceName)

  def scanMachines(self):
    """Query OCCI compute service for details of machines in this space"""
  
    try:
      result = self.httpRequest(self.computeURL,
                             headers = [ 'X-Auth-Token: ' + self.token,
                                         'User-Agent: Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type: text/occi',
                                         'Accept: text/occi',
                                       ],
                             anyStatus = True,
                             verbose = True)
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
      
#    if result['status'] == 404:
# No machines in the system??

    for machineURL in vcycle.vacutils.splitCommaHeaders(result['headers']['x-occi-location']):

      # This includes VMs that we didn't create and won't manage, to avoid going above space limit
      self.totalMachines += 1

      try:
        result = self.httpRequest(machineURL,
                             headers = [ 'X-Auth-Token: ' +self.token,
                                         'User-Agent: Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type: text/occi',
                                         'Accept: text/occi',
                                       ],
                             verbose = True)
      except Exception as e:
        raise OcciError('Cannot connect to ' + machineURL + ' (' + str(e) + ')')

      pprint.pprint(result)

      machineName = None
      occiState   = None
      uuidStr     = None
      
      for x in vcycle.vacutils.splitCommaHeaders(result['headers']['x-occi-attribute']):

        if x.startswith('occi.compute.hostname="'):
          machineName = x[23:-1]
        elif x.startswith('occi.compute.state="'):
          occiState = x[20:-1].lower()
        elif x.startswith('occi.core.id="'):
          uuidStr = x[14:-1]

      # Just in case other VMs are in this space
      if machineName[:7] != 'vcycle-':
        continue

      ip = '0.0.0.0'
      
      for x in vcycle.vacutils.splitCommaHeaders(result['headers']['link']):
        try:
          ip = re.compile('occi.networkinterface.address="([^"]*)"').findall(x)[0]
        except:
          pass

      # With OCCI will have to use our file datestamps to get transition times
      createdTime = int(time.time())
      updatedTime = int(time.time())
      startedTime = int(time.time())
      
      if occiState == 'active':
        state = vcycle.MachineState.running
      elif occiState == 'inactive':
        state = vcycle.MachineState.shutdown
      else:
        state = vcycle.MachineState.unknown

      self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                         spaceName   = self.spaceName,
                                                         state       = state,
                                                         ip          = ip,
                                                         createdTime = createdTime,
                                                         startedTime = startedTime,
                                                         updatedTime = updatedTime,
                                                         uuidStr     = uuidStr)

  def createMachine(self, vmtypeName):

    # Call the generic machine creation method
    try:
      machineName = vcycle.BaseSpace.createMachine(self, vmtypeName)
    except Exception as e:
      raise OcciError('Failed to create new machine: ' + str(e))

    # Now the OCCI-specific machine creation steps

#                    'metadata'  : { 'cern-services'   : 'false',
#                                    'machinefeatures' : 'http://'  + os.uname()[1] + '/' + machineName + '/machinefeatures',
#                                    'jobfeatures'     : 'http://'  + os.uname()[1] + '/' + machineName + '/jobfeatures',
#                                    'machineoutputs'  : 'https://' + os.uname()[1] + '/' + machineName + '/machineoutputs' }

    headers = [ 'X-Auth-Token: ' + self.token,
                'Category: compute; scheme="http://schemas.ogf.org/occi/infrastructure#"; class="kind"',
                'Category: ' + self.vmtypes[vmtypeName].flavor_name + '; scheme="http://schemas.openstack.org/template/resource#"; class="mixin"'
              ]

    try:
      headers.append('Category: user_data;scheme="http://schemas.openstack.org/compute/instance#";class="mixin"')
      headers.append('X-OCCI-Attribute: org.openstack.compute.user_data="' + base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()) + '"')
    except:
      raise OcciError('Failed to create new machine: ' + str(e))

    if self.vmtypes[vmtypeName].root_public_key:

      if self.vmtypes[vmtypeName].root_public_key[0] == '/':
        try:
          f = open(self.vmtypes[vmtypeName].root_public_key, 'r')
        except Exception as e:
          OcciError('Cannot open ' + self.vmtypes[vmtypeName].root_public_key)
      else:  
        try:
          f = open('/var/lib/vcycle/' + self.spaceName + '/' + self.vmtypeName + '/' + self.vmtypes[vmtypeName].root_public_key, 'r')
        except Exception as e:
          OcciError('Cannot open ' + self.spaceName + '/' + self.vmtypeName + '/' + self.vmtypes[vmtypeName].root_public_key)

      while True:
        try:
          line = f.read()
        except:
          raise OcciError('Cannot find ssh-rsa public key line in ' + self.vmtypes[vmtypeName].root_public_key)
        
        if line[:8] == 'ssh-rsa ':
          sshPublicKey = line.split(' ')[1]
          break

      headers.append('Category: public_key;scheme="http://schemas.openstack.org/instance/credentials#";class="mixin"')
      headers.append('X-OCCI-Attribute: org.openstack.credentials.publickey.name="' + str(time.time()).replace('.','-') + '"')
      headers.append('X-OCCI-Attribute: org.openstack.credentials.publickey.data="ssh-rsa ' + sshPublicKey + ' vcycle"')
    
    if root_image.startswith('image:'):
      headers.append('Category: ' + self.vmtypes[vmtypeName].root_image[6:].strip() + '; scheme="http://schemas.openstack.org/template/os#"; class="mixin"')
    else:
      raise OcciError('root_image must be specified with "image:" prefix')
      
    try:
      result = self.httpRequest(self.computeURL,
                             method = 'POST',
                             headers = headers)
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' for ' + vmtypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = None)

    return machineName

  def deleteOneMachine(self, machineName):

    vcycle.vacutils.logLine('Destroying ' + machineName + ' in ' + self.spaceName + ':' + 
                            str(self.machines[machineName].vmtypeName) + ', in state ' + str(self.machines[machineName].state))

    try:
      self.httpRequest(self.computeURL + '/servers/' + self.machines[machineName].uuidStr,
                    request = None,
                    method = 'DELETE',
                    headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')
