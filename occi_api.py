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

    # OpenStack-specific initialization
    try:
      self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
    except Exception as e:
      raise OcciError('tenancy_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.identityURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise OcciError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

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
  # Connect to the OpenStack service
  
    try:
      result = self.httpJSON(self.identityURL + '/tokens',
                               { 'auth' : { 'tenantName'           : self.tenancy_name, 
                                            'passwordCredentials' : { 'username' : self.username, 
                                                                      'password' : self.password 
                                                                    }
                                          }
                               } )
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.identityURL + ' (' + str(e) + ')')
 
    self.token      = str(result['response']['access']['token']['id'])
    self.computeURL = None
    self.imageURL   = None
    
    for endpoint in result['response']['access']['serviceCatalog']:
      if endpoint['type'] == 'compute':
        self.computeURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'image':
        self.imageURL = str(endpoint['endpoints'][0]['publicURL'])
        
    if not self.computeURL:
      raise OcciError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise OcciError('No image service URL found from ' + self.identityURL)

    vcycle.vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vcycle.vacutils.logLine('computeURL = ' + self.computeURL)
    vcycle.vacutils.logLine('imageURL   = ' + self.imageURL)

  def scanMachines(self):
    """Query OpenStack compute service for details of machines in this space"""
  
    try:
      result = self.httpJSON(self.computeURL + '/servers/detail',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneServer in result['response']['servers']:

      # This includes VMs that we didn't create and won't manage, to avoid going above space limit
      self.totalMachines += 1

      # Just in case other VMs are in this space
      if oneServer['name'][:7] != 'vcycle-':
        continue

      uuidStr = str(oneServer['id'])

      try:
        ip = str(oneServer['addresses']['CERN_NETWORK'][0]['addr'])
      except:
        try:
          ip = str(oneServer['addresses']['novanetwork'][0]['addr'])
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

      self.machines[oneServer['name']] = vcycle.shared.Machine(name        = oneServer['name'],
                                                               spaceName   = self.spaceName,
                                                               state       = state,
                                                               ip          = ip,
                                                               createdTime = createdTime,
                                                               startedTime = startedTime,
                                                               updatedTime = updatedTime,
                                                               uuidStr     = uuidStr)

  def getFlavorID(self, vmtypeName):
    """Get the "flavor" ID (## We're all living in Amerika! ##)"""
  
    if hasattr(self.vmtypes[vmtypeName], '_flavorID'):
      if self.vmtypes[vmtypeName]._flavorID:
        return self.vmtypes[vmtypeName]._flavorID
      else:
        raise OcciError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')
      
    try:
      result = self.httpJSON(self.computeURL + '/flavors',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
    
    try:
      for flavor in result['response']['flavors']:
        if flavor['name'] == self.vmtypes[vmtypeName].flavor_name:
          self.vmtypes[vmtypeName]._flavorID = str(flavor['id'])
          return self.vmtypes[vmtypeName]._flavorID
    except:
      pass
        
    raise OcciError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')

  def getImageID(self, vmtypeName):
    """Get the image ID"""

    # If we already know the image ID, then just return it
    if hasattr(self.vmtypes[vmtypeName], '_imageID'):
      if self.vmtypes[vmtypeName]._imageID:
        return self.vmtypes[vmtypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise OcciError('Image "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' not available!')

    # Get the existing images for this tenancy
    try:
      result = self.httpJSON(self.computeURL + '/images/detail',
                             headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    # Specific image, not managed by Vcycle, lookup ID
    if self.vmtypes[vmtypeName].root_image[:6] == 'image:':
      for image in result['response']['images']:
         if self.vmtypes[vmtypeName].root_image[6:] == image['name']:
           self.vmtypes[vmtypeName]._imageID = str(image['id'])
           return self.vmtypes[vmtypeName]._imageID

      raise OcciError('Image "' + self.vmtypes[vmtypeName].root_image[6:] + '" for vmtype ' + vmtypeName + ' not available!')

    # Always store/make the image name
    if self.vmtypes[vmtypeName].root_image[:7] == 'http://' or \
       self.vmtypes[vmtypeName].root_image[:8] == 'https://' or \
       self.vmtypes[vmtypeName].root_image[0] == '/':
      imageName = self.vmtypes[vmtypeName].root_image
    else:
      imageName = '/var/lib/vcycle/' + self.spaceName + '/' + vmtypeName + '/' + self.vmtypes[vmtypeName].root_image

    # Find the local copy of the image file
    if not hasattr(self.vmtypes[vmtypeName], '_imageFile'):

      if self.vmtypes[vmtypeName].root_image[:7] == 'http://' or \
         self.vmtypes[vmtypeName].root_image[:8] == 'https://':

        try:
          imageFile = vcycle.vacutils.getRemoteRootImage(self.vmtypes[vmtypeName].root_image,
                                         '/var/lib/vcycle/imagecache', 
                                         '/var/lib/vcycle/tmp')

          imageLastModified = int(os.stat(imageFile).st_mtime)
        except Exception as e:
          raise OcciError('Failed fetching ' + self.vmtypes[vmtypeName].root_image + ' (' + str(e) + ')')

        self.vmtypes[vmtypeName]._imageFile = imageFile
 
      elif self.vmtypes[vmtypeName].root_image[0] == '/':
        
        try:
          imageLastModified = int(os.stat(self.vmtypes[vmtypeName].root_image).st_mtime)
        except Exception as e:
          raise OcciError('Image file "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' does not exist!')

        self.vmtypes[vmtypeName]._imageFile = self.vmtypes[vmtypeName].root_image

      else: # root_image is not an absolute path, but imageName is
        
        try:
          imageLastModified = int(os.stat(imageName).st_mtime)
        except Exception as e:
          raise OcciError('Image file "' + self.vmtypes[vmtypeName].root_image +
                            '" does not exist in /var/lib/vcycle/' + self.spaceName + '/' + vmtypeName + ' !')

        self.vmtypes[vmtypeName]._imageFile = imageName

    else:
      imageLastModified = int(os.stat(self.vmtypes[vmtypeName]._imageFile).st_mtime)

    # Go through the existing images looking for a name and time stamp match
# We should delete old copies of the current image name if we find them here
#    pprint.pprint(response)
    for image in result['response']['images']:
      try:
         if image['name'] == imageName and \
            image['status'] == 'ACTIVE' and \
            image['metadata']['last_modified'] == str(imageLastModified):
           self.vmtypes[vmtypeName]._imageID = str(image['id'])
           return self.vmtypes[vmtypeName]._imageID
      except:
        pass

    vcycle.vacutils.logLine('Image "' + self.vmtypes[vmtypeName].root_image + '" not found in image service, so uploading')

    # Try to upload the image
    try:
      self.vmtypes[vmtypeName]._imageID = self.uploadImage(self.vmtypes[vmtypeName]._imageFile, imageName, imageLastModified)
      return self.vmtypes[vmtypeName]._imageID
    except Exception as e:
      raise OcciError('Failed to upload image file ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageLastModified, verbose = False):

    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise OcciError('Failed to open image file ' + imageName + ' (' + str(e) + ')')

    self.curl.setopt(pycurl.READFUNCTION,   f.read)
    self.curl.setopt(pycurl.UPLOAD,         True)
    self.curl.setopt(pycurl.CUSTOMREQUEST,  'POST')
    self.curl.setopt(pycurl.URL,            self.imageURL + '/v1/images')
    self.curl.setopt(pycurl.USERAGENT,      'Vcycle ' + vcycleVersion)
    self.curl.setopt(pycurl.TIMEOUT,        30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)

    self.curl.setopt(pycurl.HTTPHEADER,
                     [ 'x-image-meta-disk_format: raw', # <-- 'raw' for hdd; 'iso' for iso
                       'Content-Type: application/octet-stream',
                       'Accept: application/json',
                       'Transfer-Encoding: chunked',
                       'x-image-meta-container_format: bare',
                       'x-image-meta-is_public: False',                       
                       'x-image-meta-name: ' + imageName,
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
      raise OcciError('Failed uploadimg image to ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise OcciError('Upload to ' + url + ' returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise OcciError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')
    
    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise OcciError('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

  def getKeyPairName(self, vmtypeName):
    """Get the key pair name from root_public_key"""

    if hasattr(self.vmtypes[vmtypeName], '_keyPairName'):
      if self.vmtypes[vmtypeName]._keyPairName:
        return self.vmtypes[vmtypeName]._keyPairName
      else:
        raise OcciError('Key pair "' + self.vmtypes[vmtypeName].root_public_key + '" for vmtype ' + vmtypeName + ' not available!')
      
    # Get the ssh public key from the root_public_key file
        
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
        sshPublicKey =  line.split(' ')[1]
        break

    # Check if public key is there already

    try:
      result = self.httpJSON(self.computeURL + '/os-keypairs',
                             headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for keypair in result['response']['keypairs']:
      try:
        if 'ssh-rsa ' + sshPublicKey + ' vcycle' == keypair['keypair']['public_key']:
          self.vmtypes[vmtypeName]._keyPairName = str(keypair['keypair']['name'])
          return self.vmtypes[vmtypeName]._keyPairName
      except:
        pass
      
    # Not there so we try to add it
    
    keyName = str(time.time()).replace('.','-')

    try:
      result = self.httpJSON(self.computeURL + '/os-keypairs',
                               { 'keypair' : { 'name'       : keyName,
                                               'public_key' : 'ssh-rsa ' + sshPublicKey + ' vcycle'
                                             }
                               },
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.vmtypes[vmtypeName].root_public_key + ' in ' + self.spaceName)

    self.vmtypes[vmtypeName]._keyPairName = keyName
    return self.vmtypes[vmtypeName]._keyPairName

  def createMachine(self, vmtypeName):

    # Call the generic machine creation method
    try:
      machineName = vcycle.BaseSpace.createMachine(self, vmtypeName)
    except Exception as e:
      raise OcciError('Failed to create new machine: ' + str(e))

    # Now the OpenStack-specific machine creation steps

    try:
      request = { 'server' : 
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(vmtypeName),
                    'flavorRef' : self.getFlavorID(vmtypeName),
                    'metadata'  : { 'cern-services'   : 'false',
                                    'machinefeatures' : 'http://'  + os.uname()[1] + '/' + machineName + '/machinefeatures',
                                    'jobfeatures'     : 'http://'  + os.uname()[1] + '/' + machineName + '/jobfeatures',
                                    'machineoutputs'  : 'https://' + os.uname()[1] + '/' + machineName + '/machineoutputs' }
                  }    
                }

      if self.vmtypes[vmtypeName].root_public_key:
        request['server']['key_name'] = self.getKeyPairName(vmtypeName)

    except Exception as e:
      raise OcciError('Failed to create new machine: ' + str(e))

    try:
      result = self.httpJSON(self.computeURL + '/servers',
                             request,
                             headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + str(result['response']['server']['id']) + ') for ' + vmtypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                       spaceName   = self.spaceName,
                                                       state       = vcycle.MachineState.starting,
                                                       ip          = '0.0.0.0',
                                                       createdTime = int(time.time()),
                                                       startedTime = None,
                                                       updatedTime = int(time.time()),
                                                       uuidStr     = None)

    return machineName

