#!/usr/bin/python
#
#  google_api.py - a Google plugin for Vcycle
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
import M2Crypto
import hashlib

import vcycle.vacutils

def _emptyCallback1(p1, p2):
  return

class GoogleError(Exception):
  pass

class GoogleSpace(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName)

    # Google-specific initialization

    # Always has to be an explicit maximum, so default 1 if not given in [space ...] or [machinetype ...]
    if self.max_processors is None:
      self.max_processors = 1
    
    try:
      self.project_id = parser.get(spaceSectionName, 'project_id')
    except Exception as e:
      raise GoogleError('project_id is required in Google [space ' + spaceName + '] (' + str(e) + ')')

#    try:
#      self.domain_name = parser.get(spaceSectionName, 'domain_name')
#    except Exception as e:
#      self.domain_name = 'default'
#
#    try:
#      self.network_uuid = parser.get(spaceSectionName, 'network_uuid')
#    except Exception as e:
#      self.network_uuid = None

    try:
      self.zones = parser.get(spaceSectionName, 'zones').split()
    except Exception as e:
      raise GoogleError('The zones option is required in Google [space ' + spaceName + '] (' + str(e) + ')')

#    try:
#      self.identityURL = parser.get(spaceSectionName, 'url')
#    except Exception as e:
#      raise GoogleError('url is required in Google [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.client_email = parser.get(spaceSectionName, 'client_email')
    except Exception as e:
      self.client_email = None
      
    try:
      self.private_key = parser.get(spaceSectionName, 'private_key')
    except Exception as e:
      self.private_key = None

  def _getAccessToken(self):
    # https://developers.google.com/identity/protocols/OAuth2ServiceAccount#authorizingrequests

    scope = 'https://www.googleapis.com/auth/compute'
    tokenURL = 'https://accounts.google.com/o/oauth2/token'

    # Create encoded JWT header
    headerBase64 = base64.urlsafe_b64encode('{"alg": "RS256", "typ": "JWT"}')

    # Create encoded JWT claimset (600 seconds lifetime)
    claimsetBase64 = base64.urlsafe_b64encode('{"iss": "%s", "scope": "%s", "aud": "%s", "exp": %d, "iat": %d}' % 
                                              (self.client_email,
                                               scope, 
                                               tokenURL, 
                                               int(time.time()) + 600,
                                               int(time.time())))

    # Create SHA256 hash of header and claimset
    sha256HeaderClaimset = hashlib.sha256(headerBase64 + "." + claimsetBase64).digest()

    # Load private key into an M2Crypto object
    privateKey = M2Crypto.RSA.load_key_string(self.private_key, _emptyCallback1)

    # Create signature of the hash using the private key
    signatureBase64 = base64.urlsafe_b64encode(privateKey.sign(sha256HeaderClaimset, 'sha256'))

    # HTTP POST to get the access_token
    try:
      result = self.httpRequest(tokenURL, 
                                formRequest = { 'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                                                'assertion' : headerBase64 + "." + claimsetBase64 + "." + signatureBase64 }
                               )
    except Exception as e:
      raise GoogleError('Cannot connect to ' + tokenURL + ' to get OAUTH access_token (' + str(e) + ')')
    
    try:
      accessToken = str(result['response']['access_token'])
    except Exception as e:
      raise GoogleError('Failed to get OAUTH access_token from ' + tokenURL + ' (' + str(e) + ')')
    
    return accessToken

  def connectXXX(self):
  # Wrapper around the connect methods and some common post-connection updates
  

    # Build dictionary of flavor details using API
    self._getFlavors()

    # Try to get the limit on the number of processors in this project
    processorsLimit =  self._getProcessorsLimit()

    # Try to use it for this space
    if self.max_processors is None:
      vcycle.vacutils.logLine('No limit on processors set in Vcycle configuration')
      if processorsLimit is not None:
        vcycle.vacutils.logLine('Processors limit set to %d from Google' % processorsLimit)
        self.max_processors = processorsLimit
    else:
      vcycle.vacutils.logLine('Processors limit set to %d in Vcycle configuration' % self.max_processors)
      
    # Try to update processors_per_machine and rss_bytes_per_processor from flavor definitions
    for machinetypeName in self.machinetypes:
      try:
        flavorID = self.getFlavorID(self.machinetypes[machinetypeName].flavor_name)
      except:
        continue

      try:
        self.machinetypes[machinetypeName].processors_per_machine = self.flavors[flavorID]['processors']
      except:
        pass

      try:      
        self.machinetypes[machinetypeName].rss_bytes_per_processor = (self.flavors[flavorID]['mb'] * 1048576) / self.machinetypes[machinetypeName].processors
      except:
        pass

  def connect(self):
    """Connect to the Google compute cloud service"""

    self.accessToken = self._getAccessToken()
    vcycle.vacutils.logLine('Connected to Google compute cloud service for space ' + self.spaceName)
    
    for machinetypeName in self.machinetypes:
      try:
        self.machinetypes[machinetypeName].processors_per_machine = 1
      except:
        pass

  def _getFlavors(self):
    """Query Google to get details of flavors defined for this project"""

    self.flavors = {}
    
    try:
      result = self.httpRequest(self.computeURL + '/flavors/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise GoogleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
      
    for oneFlavor in result['response']['flavors']:
     
#      print str(oneFlavor)
      
      flavor = {}
      flavor['mb']          = oneFlavor['ram']
      flavor['flavor_name'] = oneFlavor['name']
      flavor['processors']  = oneFlavor['vcpus']

      self.flavors[oneFlavor['id']] = flavor

#    print str(self.flavors)

  def _getProcessorsLimit(self):
    """Query Google to get processor limit for this project"""
    
    try:
      result = self.httpRequest(self.computeURL + '/limits',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise GoogleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
      
    try:
      return int(result['response']['limits']['absolute']['maxTotalCores'])
    except:
      return None 
     
  def scanMachines(self):
    """Query Google compute service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for 
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces

    try:
      result = self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/aggregated/instances' % self.project_id,
                                headers = [ 'Authorization: Bearer ' + self.accessToken ])
    except Exception as e:
      raise GoogleError('Cannot get instances list (' + str(e) + ')')
      
    for oneZone in result['response']['items']:
    
      if 'instances' not in result['response']['items'][oneZone]:
        continue

      if oneZone.startswith('zones'):
        zone = oneZone[6:]
      else:
        zone = oneZone
    
      for oneMachine in result['response']['items'][oneZone]['instances']:
      
        pprint.pprint(oneMachine)
      
        machineName = str(oneMachine['name'])
#        flavorID = str(oneMachine['machineType'])
        processors = 1
       
        # Just in case other VMs are in this space
        if machineName[:7] != 'vcycle-':
          # Still count VMs that we didn't create and won't manage, to avoid going above space limit
          self.totalProcessors += processors
          continue

        id = str(oneMachine['id'])

        # Try to get the IP address. Always use the zeroth member of the earliest network
        try:
          ip = str(oneMachine['networkInterfaces'][0]['accessConfigs'][0]['natIP'])
        except:
          ip = '0.0.0.0'

        try:
          createdTime = calendar.timegm(time.strptime(str(oneMachine['creationTimestamp']), "%Y-%m-%dT%H:%M:%S-"))
        except:
          createdTime = None  
        
        updatedTime  = createdTime

        try:
          startedTime = calendar.timegm(time.strptime(str(oneServer['OS-SRV-USG:launched_at']).split('.')[0], "%Y-%m-%dT%H:%M:%S"))
        except:
          startedTime = None

        status     = str(oneMachine['status'])

        try:
          machinetypeName = str(oneServer['metadata']['machinetype'])
        except:
          machinetypeName = None

        if status == 'RUNNING':
          state = vcycle.MachineState.running
        elif status == 'TERMINATED':
          state = vcycle.MachineState.shutdown
        elif status == 'PENDING':
          state = vcycle.MachineState.starting
        elif status == 'STOPPING':
          state = vcycle.MachineState.deleting
#        elif status == 'ERROR':
#          state = vcycle.MachineState.failed
#        elif status == 'DELETED':
#          state = vcycle.MachineState.deleting
        else:
          state = vcycle.MachineState.unknown

        self.machines[machineName] = vcycle.shared.Machine(name             = machineName,
                                                           spaceName        = self.spaceName,
                                                           state            = state,
                                                           ip               = ip,
                                                           createdTime      = createdTime,
                                                           startedTime      = startedTime,
                                                           updatedTime      = updatedTime,
                                                           uuidStr          = id,
                                                           machinetypeName  = machinetypeName,
                                                           zone             = zone)

  def getFlavorID(self, flavorName):
    """Get the "flavor" ID"""
    
    for flavorID in self.flavors:
      if self.flavors[flavorID]['flavor_name'] == flavorName:
        return flavorID

    raise GoogleError('Flavor "' + flavorName + '" not available!')

  def getImageID(self, machinetypeName):
    """Get the image ID"""

    # If we already know the image ID, then just return it
    if hasattr(self.machinetypes[machinetypeName], '_imageID'):
      if self.machinetypes[machinetypeName]._imageID:
        return self.machinetypes[machinetypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise GoogleError('Image "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' not available!')

    # Get the existing images for this tenancy
    try:
      result = self.httpRequest(self.computeURL + '/images/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise GoogleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    # Specific image, not managed by Vcycle, lookup ID
    if self.machinetypes[machinetypeName].root_image[:6] == 'image:':
      for image in result['response']['images']:
         if self.machinetypes[machinetypeName].root_image[6:] == image['name']:
           self.machinetypes[machinetypeName]._imageID = str(image['id'])
           return self.machinetypes[machinetypeName]._imageID

      raise GoogleError('Image "' + self.machinetypes[machinetypeName].root_image[6:] + '" for machinetype ' + machinetypeName + ' not available!')

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
                                         '/var/lib/vcycle/tmp',
                                         'Vcycle ' + vcycle.shared.vcycleVersion)

          imageLastModified = int(os.stat(imageFile).st_mtime)
        except Exception as e:
          raise GoogleError('Failed fetching ' + self.machinetypes[machinetypeName].root_image + ' (' + str(e) + ')')

        self.machinetypes[machinetypeName]._imageFile = imageFile
 
      elif self.machinetypes[machinetypeName].root_image[0] == '/':
        
        try:
          imageLastModified = int(os.stat(self.machinetypes[machinetypeName].root_image).st_mtime)
        except Exception as e:
          raise GoogleError('Image file "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' does not exist!')

        self.machinetypes[machinetypeName]._imageFile = self.machinetypes[machinetypeName].root_image

      else: # root_image is not an absolute path, but imageName is
        
        try:
          imageLastModified = int(os.stat(imageName).st_mtime)
        except Exception as e:
          raise GoogleError('Image file "' + self.machinetypes[machinetypeName].root_image +
                            '" does not exist in /var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + machinetypeName + '/files/ !')

        self.machinetypes[machinetypeName]._imageFile = imageName

    else:
      imageLastModified = int(os.stat(self.machinetypes[machinetypeName]._imageFile).st_mtime)

    # Go through the existing images looking for a name and time stamp match
# We should delete old copies of the current image name if we find them here
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
        raise GoogleError('Failed to verify signature/cert for ' + self.machinetypes[machinetypeName].root_image)
      elif re.search(self.machinetypes[machinetypeName].cernvm_signing_dn,  cernvmDict['dn']) is None:
        raise GoogleError('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + self.machinetypes[machinetypeName].cernvm_signing_dn)
      else:
        vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

    # Try to upload the image
    try:
      self.machinetypes[machinetypeName]._imageID = self.uploadImage(self.machinetypes[machinetypeName]._imageFile, imageName, imageLastModified)
      return self.machinetypes[machinetypeName]._imageID
    except Exception as e:
      raise GoogleError('Failed to upload image file ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageLastModified, verbose = False):

    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise GoogleError('Failed to open image file ' + imageName + ' (' + str(e) + ')')

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
      raise GoogleError('Failed uploadimg image to ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise GoogleError('Upload to ' + url + ' returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise GoogleError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')
    
    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise GoogleError('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

  def getKeyPairName(self, machinetypeName):
    """Get the key pair name from root_public_key"""

    if hasattr(self.machinetypes[machinetypeName], '_keyPairName'):
      if self.machinetypes[machinetypeName]._keyPairName:
        return self.machinetypes[machinetypeName]._keyPairName
      else:
        raise GoogleError('Key pair "' + self.machinetypes[machinetypeName].root_public_key + '" for machinetype ' + machinetypeName + ' not available!')
      
    # Get the ssh public key from the root_public_key file
        
    if self.machinetypes[machinetypeName].root_public_key[0] == '/':
      try:
        f = open(self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        GoogleError('Cannot open ' + self.machinetypes[machinetypeName].root_public_key)
    else:  
      try:
        f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        GoogleError('Cannot open /var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key)

    while True:
      try:
        line = f.read()
      except:
        raise GoogleError('Cannot find ssh-rsa public key line in ' + self.machinetypes[machinetypeName].root_public_key)
        
      if line[:8] == 'ssh-rsa ':
        sshPublicKey =  line.split(' ')[1]
        break

    # Check if public key is there already

    try:
      result = self.httpRequest(self.computeURL + '/os-keypairs',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise GoogleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for keypair in result['response']['keypairs']:
      try:
        if 'ssh-rsa ' + sshPublicKey + ' vcycle' == keypair['keypair']['public_key']:
          self.machinetypes[machinetypeName]._keyPairName = str(keypair['keypair']['name'])
          return self.machinetypes[machinetypeName]._keyPairName
      except:
        pass
      
    # Not there so we try to add it
    
    keyName = str(time.time()).replace('.','-')

    try:
      result = self.httpRequest(self.computeURL + '/os-keypairs',
                                jsonRequest = { 'keypair' : { 'name'       : keyName,
                                                               'public_key' : 'ssh-rsa ' + sshPublicKey + ' vcycle'
                                                            }
                                              },
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise GoogleError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.machinetypes[machinetypeName].root_public_key + ' in ' + self.spaceName)

    self.machinetypes[machinetypeName]._keyPairName = keyName
    return self.machinetypes[machinetypeName]._keyPairName

  def createMachine(self, machineName, machinetypeName, zone):
    # Google-specific machine creation steps

    try:    
      if self.machinetypes[machinetypeName].remote_joboutputs_url:
        joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
      else:
        joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'

      request = { 'name'        : machineName,
                  'machineType' : 'zones/%s/machineTypes/%s' % (zone, self.machinetypes[machinetypeName].flavor_name),
                  'disks' : [ 
                              { 
                                'initializeParams' : { 'sourceImage' : 'global/images/' + self.machinetypes[machinetypeName].root_image },
                                'boot' : True
                              }
                            ],
                  'networkInterfaces' : [ 
                                          {
                                            'network' : 'global/networks/default',
                                            'accessConfigs' : [
                                                                { 'name': 'external-nat',
                                                                  'type': 'ONE_TO_ONE_NAT' }
                                                              ]
                                          }
                                        ],
                  'metadata': {
                                'items': [
                                           { 'key'   : 'cvm-user-data',
                                             'value' : 'IyEgL2Jpbi9iYXNoCnNlZCAtaSAnczpeREVGX01EX1ZFUlNJT04uKjpERUZfTURfVkVSU0lPTiA9ICIwLjEvbWV0YS1kYXRhL2F0dHJpYnV0ZXMiOicgXAogIC91c3IvbGliL3B5dGhvbjIuNi9zaXRlLXBhY2thZ2VzL2Nsb3VkaW5pdC9zb3VyY2VzL0RhdGFTb3VyY2VFYzIucHkgClthbWljb25maWddCnBsdWdpbnM9Y2VybnZtCltjZXJudm1dCnJlcG9zaXRvcmllcyA9IGdyaWQKcHJveHk9RElSRUNUClt1Y2VybnZtLWJlZ2luXQpyZXNpemVfcm9vdGZzPW9mZgpjdm1mc19odHRwX3Byb3h5PURJUkVDVApbdWNlcm52bS1lbmRdCg==' },
#                                             'value' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()) },
                                           { 'key'   : 'name',
                                             'value' : machineName },
                                           { 'key'   : 'machinetype',
                                             'value' :  machinetypeName },
                                           { 'key'   : 'machinefeatures',
                                             'value' : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures' },
                                           { 'key'   : 'jobfeatures',
                                             'value' :  'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures' },
                                           { 'key'   : 'joboutputs',
                                             'value' :  joboutputsURL }
                                         ]
                              }
                }

      pprint.pprint(request)

#      request = { 'server' : 
#                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
#                    'name'      : machineName,
#                    'imageRef'  : self.getImageID(machinetypeName),
#                    'flavorRef' : self.getFlavorID(self.machinetypes[machinetypeName].flavor_name),
#                    'metadata'  : { 'cern-services'   : 'false',
#                                    'name'	      : machineName,
#                                    'machinetype'     : machinetypeName,
#                                    'machinefeatures' : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures',
#                                    'jobfeatures'     : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures',
#                                    'machineoutputs'  : joboutputsURL,
#                                    'joboutputs'      : joboutputsURL  }
#                    # Changing over from machineoutputs to joboutputs, so we set both in the metadata for now, 
#                    # but point them both to the joboutputs directory that we now provide
#                  }    
#                }

       


#      if self.network_uuid:
#        request['server']['networks'] = [{"uuid": self.network_uuid}]
#        vcycle.vacutils.logLine('Will use network %s for %s' % (self.network_uuid, machineName))

#      if self.zones:
#        request['server']['availability_zone'] = random.choice(self.zones)
#        vcycle.vacutils.logLine('Will request %s be created in zone %s of space %s' % (machineName, request['server']['availability_zone'], self.spaceName))

#      if self.machinetypes[machinetypeName].root_public_key:
#        request['server']['key_name'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise GoogleError('Failed to create new machine request for %s: %s' % (machineName, str(e)))

    try:
      result = self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances' % (self.project_id, zone),
                                jsonRequest = request,
                                headers = [ 'Authorization: Bearer ' + self.accessToken ])
    except Exception as e:
      raise GoogleError('Cannot create VM (' + str(e) + ')')
      
    pprint.pprint(result)

    vcycle.vacutils.logLine('Created ' + machineName + ' for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name            = machineName,
                                                       spaceName       = self.spaceName,
                                                       state           = vcycle.MachineState.starting,
                                                       ip              = '0.0.0.0',
                                                       createdTime     = int(time.time()),
                                                       startedTime     = None,
                                                       updatedTime     = int(time.time()),
                                                       uuidStr         = None,
                                                       machinetypeName = machinetypeName,
                                                       zone            = zone)

  def deleteOneMachine(self, machineName):

    try:
      self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s' % (self.project_id, self.machines[machineName].zone, machineName),
                       method = 'DELETE',
                       headers = [ 'Authorization: Bearer ' + self.accessToken ])
                       
    except Exception as e:
      raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' (' + str(e) + ')')
