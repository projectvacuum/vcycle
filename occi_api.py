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
      result = self.httpJSON(self.queryURL + '/-/', method = 'HEAD', anyStatus = True)
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
      result = self.httpJSON(keystoneURL + 'v2.0/tokens',
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
      result = self.httpJSON(self.queryURL + '/-/',
                             headers = { 'X-Auth-Token'  : self.token,
                                         'User-Agent'	 : 'Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type'	 : 'text/occi',
                                         'Accept'	 : 'text/occi',
                                       })
    except Exception as e:
      raise OcciError('Cannot reconnect to ' + self.queryURL + ' (' + str(e) + ')')
                                                       
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
      result = self.httpJSON(self.computeURL,
                             headers = { 'X-Auth-Token'  : self.token,
                                         'User-Agent'	 : 'Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type'	 : 'text/occi',
                                         'Accept'	 : 'text/occi',
                                       },
                             verbose = True)
    except Exception as e:
      raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for machineURL in vcycle.vacutils.splitCommaHeaders(result['headers']['x-occi-location']):

      # This includes VMs that we didn't create and won't manage, to avoid going above space limit
      self.totalMachines += 1

      try:
        result = self.httpJSON(machineURL,
                             headers = { 'X-Auth-Token'  : self.token,
                                         'User-Agent'	 : 'Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                                         'Content-Type'	 : 'text/occi',
                                         'Accept'	 : 'text/occi',
                                       },
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
                    'flavorRef' : '123',
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

  def deleteOneMachine(self, machineName):

    vcycle.vacutils.logLine('Destroying ' + machineName + ' in ' + self.spaceName + ':' + 
                            str(self.machines[machineName].vmtypeName) + ', in state ' + str(self.machines[machineName].state))

    try:
      self.httpJSON(self.computeURL + '/servers/' + self.machines[machineName].uuidStr,
                    request = None,
                    method = 'DELETE',
                    headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')
