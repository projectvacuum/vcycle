#!/usr/bin/python
#
#  ec2_api.py - an EC2 plugin for Vcycle
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
import hmac
import shutil
import string
import pycurl
import random
import base64
import urllib
import datetime
import hashlib
import StringIO
import tempfile
import calendar

import vcycle.vacutils

class Ec2Error(Exception):
  pass

class Ec2Space(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    # EC2-specific initialization
    try:
      self.access_key = parser.get(spaceSectionName, 'access_key')
    except Exception as e:
      raise Ec2Error('access_key is required in EC2 [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.secret_key = parser.get(spaceSectionName, 'secret_key')
    except Exception as e:
      raise Ec2Error('secret_key is required in EC2 [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.url = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise Ec2Error('url is required in EC2 [space ' + spaceName + '] (' + str(e) + ')')

    if parser.has_option(spaceSectionName, 'version'):
      self.version = parser.get(spaceSectionName, 'version').strip()
    else:
      self.version = '2010-08-31'

    if parser.has_option(spaceSectionName, 'region'):
      self.region = parser.get(spaceSectionName, 'region').strip()
    else:
      self.region = 'us-east-1'

    if parser.has_option(spaceSectionName, 'service'):
      self.service = parser.get(spaceSectionName, 'service').strip()
    else:
      self.service = 'openstack'

  def ec2Sign(self, key, message):
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()

  def ec2SignatureKey(self, dateStamp):
    kDate    = self.ec2Sign(('AWS4' + self.secret_key).encode('utf-8'), dateStamp)
    kRegion  = self.ec2Sign(kDate,    self.region)
    kService = self.ec2Sign(kRegion,  self.service)
    kSigning = self.ec2Sign(kService, 'aws4_request')
    return kSigning

  def ec2Request(self, formRequest = None, verbose = False, anyStatus = False):
    # Wrapper around BaseSpace.httpRequest() that adds correct EC2 Authorization: header

    amzTime      = datetime.datetime.utcnow()
    amzDate      = amzTime.strftime('%Y%m%dT%H%M%SZ')
    amzDateStamp = amzTime.strftime('%Y%m%d')

    uri  = '/' + '/'.join(self.url.split('/')[3:])
    host = self.url.split('/')[2]

    signedHeaderNames      = ''
    signedHeaderNameValues = ''
    headersList            = []

    # Add the headers in alphabetical order

    signedHeaderNames      += 'host'
    signedHeaderNameValues += 'host:' + host + '\n'
    headersList.append('Host: ' + host)

    signedHeaderNames      += ';x-amz-date'
    signedHeaderNameValues += 'x-amz-date:' + amzDate + '\n'
    headersList.append('X-Amz-Date: ' + amzDate)

    # Now build up the signature bit by bit

    formRequestBody  = urllib.urlencode(formRequest)
    canonicalRequest = 'POST\n' + uri + '\n\n' + signedHeaderNameValues + '\n' + signedHeaderNames + '\n' + hashlib.sha256(formRequestBody).hexdigest()

    credentialScope = amzDateStamp + '/' + self.region + '/' + self.service + '/' + 'aws4_request'
    stringToSign    = 'AWS4-HMAC-SHA256\n' +  amzDate + '\n' +  credentialScope + '\n' +  hashlib.sha256(canonicalRequest).hexdigest()
    signature       = hmac.new(self.ec2SignatureKey(amzDateStamp), (stringToSign).encode('utf-8'), hashlib.sha256).hexdigest()

    authorizationHeaderValue = 'AWS4-HMAC-SHA256 Credential=' + self.access_key + '/' + credentialScope + ', SignedHeaders=' + signedHeaderNames + ', Signature=' + signature
    headersList.append('Authorization: ' + authorizationHeaderValue)

    return vcycle.BaseSpace.httpRequest(self, self.url, formRequest = formRequestBody, headers = headersList, verbose = verbose, method = 'POST', anyStatus = anyStatus)

  def scanMachines(self):
    """Query EC2 service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces

    try:
      result = self.ec2Request( formRequest = { 'Action' : 'DescribeInstances', 'Version' : self.version }, verbose = False )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    # Convert machines from None to an empty dictionary since we successfully connected
    self.machines = {}

    for item1 in result['response']['DescribeInstancesResponse']['reservationSet'][0]['item']:
     for oneServer in item1['instancesSet'][0]['item']:

      self.totalProcessors += 1 # FIXME: GET THE REAL NUMBER NOT JUST 1

      instanceId      = oneServer['instanceId'][0]['#text']
      instanceState   = oneServer['instanceState'][0]['name'][0]['#text']
      machineName     = None
      machinetypeName = None

      if 'tagSet' in oneServer and 'item' in oneServer['tagSet'][0]:
        for keyValue in oneServer['tagSet'][0]['item']:
          key   = keyValue['key'  ][0]['#text']
          value = keyValue['value'][0]['#text']

          # save interesting tags (metadata)
          if key == 'name':
            machineName = value
          elif key == 'machinetype':
            machinetypeName = value

      if machineName is None:
        # if still None, then try to find by instanceId
        foundMachineNames = self.findMachinesWithFile('instance_id:' + instanceId)

        if len(foundMachineNames) == 1:
          machineName = foundMachineNames[0]

      if not machineName or not machineName.startswith('vcycle-'):
        # not one of ours
        continue

      if not machinetypeName:
        machinetypeName = self.getFileContents(machineName, 'machinetype_name')
        if not machinetypeName:
          # something weird, not ours?
          continue

      try:
        createdTime = int(self.getFileContents(machineName, 'created'))
      except:
        # something weird, not ours?
        continue

      # Try to get the IP address
      try:
        ip = str(oneServer['privateIpAddress'][0]['#text'])
      except:
        ip = '0.0.0.0'

      try:
        updatedTime = int(self.getFileContents(machineName, 'updated'))
      except:
        updatedTime = None

      try:
        startedTime = calendar.timegm(time.strptime(oneServer['launchTime'][0]['#text'], "%Y-%m-%dT%H:%M:%SZ"))
      except:
        startedTime = None

      if instanceState == 'running':
        state = vcycle.MachineState.running
      elif instanceState == 'pending':
        state = vcycle.MachineState.starting
      elif instanceState == 'stopping' or instanceState == 'stopped':
        state = vcycle.MachineState.shutdown
      elif instanceState == 'shutting-down' or instanceState == 'terminated':
        state = vcycle.MachineState.deleting
      elif instanceState == 'error':
        state = vcycle.MachineState.failed
      else:
        state = vcycle.MachineState.unknown

      if state == vcycle.MachineState.running and ('tagSet' not in oneServer or 'item' not in oneServer['tagSet'][0]):
        # Running but no tags yet, so try creating
        try:
          self.createTags(instanceId, machineName, machinetypeName)
        except Exception as e:
          vcycle.vacutils.logLine('Adding tags fails with ' + str(e))

      self.machines[machineName] = vcycle.shared.Machine(name            = machineName,
                                                         spaceName       = self.spaceName,
                                                         state           = state,
                                                         ip              = ip,
                                                         createdTime     = None,
                                                         startedTime     = None,
                                                         updatedTime     = updatedTime,
                                                         uuidStr         = instanceId,
                                                         machinetypeName = machinetypeName)

  def getImageID(self, machinetypeName):
    """Get the image ID"""

    # Specific image, not managed by Vcycle, lookup ID
    if self.machinetypes[machinetypeName].root_image[:6] == 'image:':
      return self.machinetypes[machinetypeName].root_image[6:]

    raise Ec2Error('Failed to get image ID as no image stored for machinetype ' + machinetypeName)

  def getKeyPairName(self, machinetypeName):
    """Get the key pair name from root_public_key"""

    # Look for the cached key pair
    if hasattr(self.machinetypes[machinetypeName], '_keyPairName'):
      if self.machinetypes[machinetypeName]._keyPairName:
        return self.machinetypes[machinetypeName]._keyPairName
      else:
        raise Ec2Error('Key pair "' + self.machinetypes[machinetypeName].root_public_key + '" for machinetype ' + machinetypeName + ' not available!')

    # Get the ssh public key from the root_public_key file

    if self.machinetypes[machinetypeName].root_public_key[0] == '/':
      try:
        f = open(self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        Ec2Error('Cannot open ' + self.machinetypes[machinetypeName].root_public_key)
    else:
      try:
        f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        Ec2Error('Cannot open /var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key)

    while True:
      try:
        line = f.read()
      except:
        raise Ec2Error('Cannot find ssh-rsa public key line in ' + self.machinetypes[machinetypeName].root_public_key)

      if line[:8] == 'ssh-rsa ':
        sshPublicKey = line.split(' ')[1]
        sshFingerprint = vcycle.vacutils.makeSshFingerprint(line)
        break

    # Check if public key is there already

    try:
      result = self.ec2Request( formRequest = { 'Action' : 'DescribeKeyPairs', 'Version' : self.version },
                                verbose = False )
    except Exception as e:
      raise Ec2Error('getKeyPairName cannot connect to ' + self.url + ' (' + str(e) + ')')

    for keypair in result['response']['DescribeKeyPairsResponse']['keySet'][0]['item']:
      try:
        if sshFingerprint == keypair['keyFingerprint'][0]['#text']:
          self.machinetypes[machinetypeName]._keyPairName = str(keypair['keyName'][0]['#text'])
          return self.machinetypes[machinetypeName]._keyPairName
      except:
        pass

    # Not there so we try to add it

    keyName = str(time.time()).replace('.','-')

    try:
      result = self.ec2Request(
                                formRequest =
                                  {
                                    'Action'            : 'ImportKeyPair',
                                    'Version'           : self.version,
                                    'KeyName'           : keyName,
                                    'PublicKeyMaterial' : base64.b64encode('ssh-rsa ' + sshPublicKey + ' vcycle')
                                  },
                                verbose = False
                              )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.machinetypes[machinetypeName].root_public_key + ' in ' + self.spaceName)

    self.machinetypes[machinetypeName]._keyPairName = keyName
    return self.machinetypes[machinetypeName]._keyPairName

  def createMachine(self, machineName, machinetypeName, zone = None):

    # EC2-specific machine creation steps

    try:
      formRequest = { 'Action'       : 'RunInstances',
                      'Version'      : self.version,
                      'MinCount'     : '1',
                      'MaxCount'     : '1',
                      'UserData'     : base64.b64encode(self.getFileContents(machineName, 'user_data')),
                      'ImageId'      : self.getImageID(machinetypeName),
                      'InstanceType' : self.machinetypes[machinetypeName].flavor_names[0] }

      if self.machinetypes[machinetypeName].root_public_key:
        formRequest['KeyName'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise Ec2Error('Failed to create new machine: ' + str(e))

    try:
      result = self.ec2Request( formRequest = formRequest, verbose = False )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    try:
      instanceId = result['response']['RunInstancesResponse']['instancesSet'][0]['item'][0]['instanceId'][0]['#text']
    except:
      instanceId = None
    else:
      self.setFileContents(machineName, 'instance_id:' + instanceId, machineName)
      self.setFileContents(machineName, 'instance_id', instanceId)

    try:
      privateDnsName = result['response']['RunInstancesResponse']['instancesSet'][0]['item'][0]['privateDnsName'][0]['#text']
    except:
      privateDnsName = None
    else:
      self.setFileContents(machineName, 'private_dns_name', privateDnsName)

    vcycle.vacutils.logLine('Created ' + machineName + ' ( ' + str(instanceId) + ' / ' + str(privateDnsName) + ' ) for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = instanceId,
                                                       machinetypeName  = machinetypeName)

  def createTags(self, instanceId, machineName, machinetypeName):

    try:
      result = self.ec2Request( formRequest = {
                      'Action'       : 'CreateTags',
                      'Version'      : self.version,
                      'ResourceId.1' : instanceId,
                      'Tag.1.Key'    : 'name',
                      'Tag.1.Value'  : machineName,
                      'Tag.2.Key'    : 'machinetype',
                      'Tag.2.Value'  : machinetypeName,
                      'Tag.3.Key'    : 'machinefeatures',
                      'Tag.3.Value'  : 'https://' + self.https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/machinefeatures',
                      'Tag.4.Key'    : 'jobfeatures',
                      'Tag.4.Value'  : 'https://' + self.https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/jobfeatures',
                      'Tag.5.Key'    : 'joboutputs',
                      'Tag.5.Value'  : 'https://' + self.https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/joboutputs'
                                              },
                                verbose = False )

    except Exception as e:
      raise Ec2Error('Adding tags to ' + machineName + ' (' + instanceId + ') fails with ' + str(e))

  def deleteOneMachine(self, machineName):

    try:
      instanceId = self.getFileContents(machineName, 'instance_id')
    except:
      raise Ec2Error('Cannot find instance_id when trying to delete ' + machineName)

    try:
      result = self.ec2Request( formRequest = {
                      'Action'       : 'TerminateInstances',
                      'Version'      : self.version,
                      'InstanceId.1' : instanceId
                                              },
                                verbose = False )
    except Exception as e:
      raise Ec2Error('Cannot delete ' + machineName + ' (' + instanceId + ') via ' + self.url + ' (' + str(e) + ')')

    if result['status'] == 200:
      # For EC2, we want to log the instanceId as well as the machineName
      vcycle.vacutils.logLine('Deleted ' + machineName + ' (' + instanceId + ')')
    else:
      vcycle.vacutils.logLine('Deletion of ' + machineName + ' (' + instanceId + ') fails with code ' + str(result['status']))
