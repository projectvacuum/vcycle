#!/usr/bin/python
#
#  ec2_api.py - an EC2 plugin for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-6. All rights reserved.
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

  def __init__(self, api, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, spaceName, parser, spaceSectionName)

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
    print stringToSign
    signature       = hmac.new(self.ec2SignatureKey(amzDateStamp), (stringToSign).encode('utf-8'), hashlib.sha256).hexdigest()

    authorizationHeaderValue = 'AWS4-HMAC-SHA256 Credential=' + self.access_key + '/' + credentialScope + ', SignedHeaders=' + signedHeaderNames + ', Signature=' + signature
    headersList.append('Authorization: ' + authorizationHeaderValue)

    print formRequestBody
 
    return vcycle.BaseSpace.httpRequest(self, self.url, formRequest = formRequestBody, headers = headersList, verbose = verbose, method = 'POST', anyStatus = anyStatus)

  def scanMachines(self):
    """Query EC2 service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for 
    # either (a) ignorning non-Vcycle VMs but updating self.totalMachines
    # or (b) creating a Machine object for the VM in self.spaces
  
    try:
      result = self.ec2Request( formRequest = { 'Action' : 'DescribeInstances', 'Version' : self.version }, verbose = True )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    print 'DescribeInstances result',result

# for item in result['response']['DescribeInstancesResponse']['reservationSet'][0]['item']:
#      oneServer = item['instancesSet'][0]['item'][0]

    for item1 in result['response']['DescribeInstancesResponse']['reservationSet'][0]['item']:
     for oneServer in item1['instancesSet'][0]['item']:

      self.totalMachines += 1

      print
      print '>+>+>+>',str(oneServer),'<+<+<+<+'
      
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
          print 'Found ' + instanceId + ' as ' + machineName
          
      if not machineName or not machineName.startswith('vcycle-'):
        # not one of ours
        print 'Skipping since machineName is',machineName
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

    # If we already know the image ID, then just return it
    if hasattr(self.machinetypes[machinetypeName], '_imageID'):
      if self.machinetypes[machinetypeName]._imageID:
        return self.machinetypes[machinetypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise Ec2Error('Image "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' not available!')

    # Get the existing images for this tenancy
    try:
      result = self.httpRequest(self.url,
                             headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    # Specific image, not managed by Vcycle, lookup ID
    if self.machinetypes[machinetypeName].root_image[:6] == 'image:':
      for image in result['response']['images']:
         if self.machinetypes[machinetypeName].root_image[6:] == image['name']:
           self.machinetypes[machinetypeName]._imageID = str(image['id'])
           return self.machinetypes[machinetypeName]._imageID

      raise Ec2Error('Image "' + self.machinetypes[machinetypeName].root_image[6:] + '" for machinetype ' + machinetypeName + ' not available!')

    # Always store/make the image name
    if self.machinetypes[machinetypeName].root_image[:7] == 'http://' or \
       self.machinetypes[machinetypeName].root_image[:8] == 'https://' or \
       self.machinetypes[machinetypeName].root_image[0] == '/':
      imageName = self.machinetypes[machinetypeName].root_image
    else:
      imageName = '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_image

    # Find the local copy of the image file
    if not hasattr(self.machinetypes[machinetypeName], '_imageFile'):

      if self.machinetypes[machinetypeName].root_image[:7] == 'http://' or \
         self.machinetypes[machinetypeName].root_image[:8] == 'https://':

        try:
          imageFile = vcycle.vacutils.getRemoteRootImage(self.machinetypes[machinetypeName].root_image,
                                         '/var/lib/vcycle/imagecache', 
                                         '/var/lib/vcycle/tmp')

          imageLastModified = int(os.stat(imageFile).st_mtime)
        except Exception as e:
          raise Ec2Error('Failed fetching ' + self.machinetypes[machinetypeName].root_image + ' (' + str(e) + ')')

        self.machinetypes[machinetypeName]._imageFile = imageFile
 
      elif self.machinetypes[machinetypeName].root_image[0] == '/':
        
        try:
          imageLastModified = int(os.stat(self.machinetypes[machinetypeName].root_image).st_mtime)
        except Exception as e:
          raise Ec2Error('Image file "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' does not exist!')

        self.machinetypes[machinetypeName]._imageFile = self.machinetypes[machinetypeName].root_image

      else: # root_image is not an absolute path, but imageName is
        
        try:
          imageLastModified = int(os.stat(imageName).st_mtime)
        except Exception as e:
          raise Ec2Error('Image file "' + self.machinetypes[machinetypeName].root_image +
                            '" does not exist in /var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + machinetypeName + '/files/ !')

        self.machinetypes[machinetypeName]._imageFile = imageName

    else:
      imageLastModified = int(os.stat(self.machinetypes[machinetypeName]._imageFile).st_mtime)

    # Go through the existing images looking for a name and time stamp match
# We should delete old copies of the current image name if we find them here
#    pprint.pprint(response)
    for image in result['response']['images']:
      try:
         if image['name'] == imageName and \
            image['status'] == 'ACTIVE' and \
            image['metadata']['last_modified'] == str(imageLastModified):
           self.machinetypes[machinetypeName]._imageID = str(image['id'])
           return self.machinetypes[machinetypeName]._imageID
      except:
        pass

    vcycle.vacutils.logLine('Image "' + self.machinetypes[machinetypeName].root_image + '" not found in image service, so uploading')

    if self.machinetypes[machinetypeName].cernvm_signing_dn:
      cernvmDict = vac.vacutils.getCernvmImageData(self.machinetypes[machinetypeName]._imageFile)

      if cernvmDict['verified'] == False:
        raise Ec2Error('Failed to verify signature/cert for ' + self.machinetypes[machinetypeName].root_image)
      elif re.search(self.machinetypes[machinetypeName].cernvm_signing_dn,  cernvmDict['dn']) is None:
        raise Ec2Error('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + self.machinetypes[machinetypeName].cernvm_signing_dn)
      else:
        vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

    # Try to upload the image
    try:
      self.machinetypes[machinetypeName]._imageID = self.uploadImage(self.machinetypes[machinetypeName]._imageFile, imageName, imageLastModified)
      return self.machinetypes[machinetypeName]._imageID
    except Exception as e:
      raise Ec2Error('Failed to upload image file ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageLastModified, verbose = False):

    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise Ec2Error('Failed to open image file ' + imageName + ' (' + str(e) + ')')

    self.curl.setopt(pycurl.READFUNCTION,   f.read)
    self.curl.setopt(pycurl.UPLOAD,         True)
    self.curl.setopt(pycurl.CUSTOMREQUEST,  'POST')
    self.curl.setopt(pycurl.URL,            self.imageURL + '/v1/images')
    self.curl.setopt(pycurl.USERAGENT,      'Vcycle ' + vcycle.shared.vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT,        30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)

    self.curl.setopt(pycurl.HTTPHEADER,
                     [ 'x-image-meta-disk_format: ' + ('iso' if imageName.endswith('.iso') else 'raw'), 
                        # ^^^ 'raw' for hdd; 'iso' for iso
                       'Content-Type: application/octet-stream',
                       'Accept: application/json',
                       'Transfer-Encoding: chunked',
                       'x-image-meta-container_format: bare',
                       'x-image-meta-is_public: False',                       
                       'x-image-meta-name: ' + imageName,
                       'x-image-meta-property-architecture: x86_64',
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
      raise Ec2Error('Failed uploadimg image to ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise Ec2Error('Upload to ' + url + ' returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise Ec2Error('JSON decoding of HTTP(S) response fails (' + str(e) + ')')
    
    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise Ec2Error('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

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
                                verbose = True )
    except Exception as e:
      raise Ec2Error('getKeyPairName cannot connect to ' + self.url + ' (' + str(e) + ')')

    print 'DescribeKeyPairs result',result

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
                                verbose = True
                              )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.machinetypes[machinetypeName].root_public_key + ' in ' + self.spaceName)

    self.machinetypes[machinetypeName]._keyPairName = keyName
    return self.machinetypes[machinetypeName]._keyPairName

  def createMachine(self, machineName, machinetypeName):

    # EC2-specific machine creation steps

    try:
      if self.machinetypes[machinetypeName].remote_joboutputs_url:
        joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
      else:
        joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'

      formRequest = { 'Action'       : 'RunInstances',
                      'Version'      : self.version,
                      'MinCount'     : '1',
                      'MaxCount'     : '1',
                      'UserData'     : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                      'ImageId'      : 'ami-00000381', # self.getImageID(machinetypeName),
                      'InstanceType' : self.machinetypes[machinetypeName].flavor_name }
      
      if self.machinetypes[machinetypeName].root_public_key:
        formRequest['KeyName'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise Ec2Error('Failed to create new machine: ' + str(e))

    print 'formRequest',str(formRequest)

    try:
      result = self.ec2Request( formRequest = formRequest, verbose = True )
    except Exception as e:
      raise Ec2Error('Cannot connect to ' + self.url + ' (' + str(e) + ')')

    print 'result',str(result)

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

    print 'Start of createTags',instanceId,machineName,machinetypeName
  
    if self.machinetypes[machinetypeName].remote_joboutputs_url:
      joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
    else:
      joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'

    try:
      print 'Trying ec2Request'
      result = self.ec2Request( formRequest = { 
                      'Action'       : 'CreateTags',
                      'Version'      : self.version,
                      'ResourceId.1' : instanceId,
                      'Tag.1.Key'    : 'name',
                      'Tag.1.Value'  : machineName,
                      'Tag.2.Key'    : 'machinetype',
                      'Tag.2.Value'  : machinetypeName,
                      'Tag.3.Key'    : 'machinefeatures',
                      'Tag.3.Value'  : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures',
                      'Tag.4.Key'    : 'jobfeatures',
                      'Tag.4.Value'  : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures',
                      'Tag.5.Key'    : 'machineoutputs',
                      'Tag.5.Value'  : joboutputsURL,
                      'Tag.6.Key'    : 'joboutputs',
                      'Tag.6.Value'  : joboutputsURL
                                              },
                                verbose = True )

      print 'result:',str(result)
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
                                verbose = True )
    except Exception as e:
      raise Ec2Error('Cannot delete ' + machineName + ' (' + instanceId + ') via ' + self.url + ' (' + str(e) + ')')

    if result['status'] == 200:
      # For EC2, we want to log the instanceId as well as the machineName
      vcycle.vacutils.logLine('Deleted ' + machineName + ' (' + instanceId + ')')
    else:
      vcycle.vacutils.logLine('Deletion of ' + machineName + ' (' + instanceId + ') fails with code ' + str(result['status']))
