#!/usr/bin/python
#
#  openstack_api.py - an OpenStack plugin for Vcycle
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

class OpenstackError(Exception):
  pass

class OpenstackSpace(vcycle.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName)

    # OpenStack-specific initialization
    try:
      self.project_name = parser.get(spaceSectionName, 'tenancy_name')
    except:
      try:
        self.project_name = parser.get(spaceSectionName, 'project_name')
      except Exception as e:
        raise OpenstackError('project_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')
    else:
      vcycle.vacutils.logLine('tenancy_name in [space ' + self.spaceName + '] is deprecated - please use project_name')

    try:
      self.domain_name = parser.get(spaceSectionName, 'domain_name')
    except Exception as e:
      self.domain_name = 'default'

    try:
      self.network_uuid = parser.get(spaceSectionName, 'network_uuid')
    except Exception as e:
      self.network_uuid = None

    try:
      self.identityURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise OpenstackError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.username = parser.get(spaceSectionName, 'username')
    except Exception as e:
      self.username = None
      
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
      raise OpenstackError('username or usercert/userkey is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      # We use Base64 encoding so browsing around casually 
      # doesn't reveal passwords in a memorable way. 
      self.password = base64.b64decode(parser.get(spaceSectionName, 'password_base64').strip()).strip()
    except Exception:
      self.password = ''

    if not self.apiVersion or self.apiVersion == '2' or self.apiVersion.startswith('2.'):
      self.connect = self.connectV2
    elif self.apiVersion == '3' or self.apiVersion.startswith('3.'):
      self.connect = self.connectV3
    else:
      raise OpenstackError('api_version %s not recognised' % self.apiVersion)

  def connectV2(self):
  # Connect to the OpenStack service with Identity v2

    try:
      result = self.httpRequest(self.identityURL + '/tokens',
                                jsonRequest = { 'auth' : { 'tenantName' : self.project_name, 
                                                           'passwordCredentials' : { 'username' : self.username, 'password' : self.password }
                                                         }
                                              }
                               )
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.identityURL + ' with v2 API (' + str(e) + ')')

    self.token = str(result['response']['access']['token']['id'])

    self.computeURL = None
    self.imageURL   = None
    
    for endpoint in result['response']['access']['serviceCatalog']:
      if endpoint['type'] == 'compute':
        self.computeURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'image':
        self.imageURL = str(endpoint['endpoints'][0]['publicURL'])
        
    if not self.computeURL:
      raise OpenstackError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise OpenstackError('No image service URL found from ' + self.identityURL)

    vcycle.vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vcycle.vacutils.logLine('computeURL = ' + self.computeURL)
    vcycle.vacutils.logLine('imageURL   = ' + self.imageURL)

  def connectV3(self):
  # Connect to the OpenStack service with Identity v3

    try:
      # No trailing slash of identityURL! (matches URL on Horizon Dashboard API page)
      result = self.httpRequest(self.identityURL + '/auth/tokens',
                                jsonRequest = { "auth": { "identity": { "methods" : [ "password"],
                                                                        "password": {
                                                                                      "user": {
                                                                                                "name"    : self.username,
                                                                                                "domain"  : { "name": self.domain_name },
                                                                                                "password": self.password
                                                                                              }
                                                                                    }
                                                                      },
                                                "scope": { "project": { "domain"  : { "name": self.domain_name }, "name": self.project_name } }
                                              }
                                  }
                               )
    except Exception as e:
        raise OpenstackError('Cannot connect to ' + self.identityURL + ' with v' + self.apiVersion + ' API (' + str(e) + ')')

    try:
      self.token = result['headers']['x-subject-token'][0]
    except Exception as e:
      raise OpenstackError('Cannot read X-Subject-Token: from ' + self.identityURL + ' response with v' + self.apiVersion + ' API (' + str(e) + ')')

    self.computeURL = None
    self.imageURL   = None

    # This might be a bit naive? We just keep the LAST one we see.
    for service in result['response']['token']['catalog']:

      if service['type'] == 'compute':
        for endpoint in service['endpoints']:      
          if endpoint['interface'] == 'public':
            self.computeURL = str(endpoint['url'])
      
      elif service['type'] == 'image':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public':
            self.imageURL = str(endpoint['url'])
        
    if not self.computeURL:
      raise OpenstackError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise OpenstackError('No image service URL found from ' + self.identityURL)

    vcycle.vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vcycle.vacutils.logLine('computeURL = ' + self.computeURL)
    vcycle.vacutils.logLine('imageURL   = ' + self.imageURL)

  def scanMachines(self):
    """Query OpenStack compute service for details of machines in this space"""

    # For each machine found in the space, this method is responsible for 
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces
  
    try:
      result = self.httpRequest(self.computeURL + '/servers/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneServer in result['response']['servers']:

      try:
        machineName = str(oneServer['metadata']['name'])
      except:
        machineName = oneServer['name']

      # Just in case other VMs are in this space
      if machineName[:7] != 'vcycle-':
        # Still count VMs that we didn't create and won't manage, to avoid going above space limit
        self.totalProcessors += 1 # FIXME: GET THE EXACT NUMBER FROM oneServer
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

      self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                         spaceName   = self.spaceName,
                                                         state       = state,
                                                         ip          = ip,
                                                         createdTime = createdTime,
                                                         startedTime = startedTime,
                                                         updatedTime = updatedTime,
                                                         uuidStr     = uuidStr,
                                                         machinetypeName  = machinetypeName)

  def getFlavorID(self, machinetypeName):
    """Get the "flavor" ID"""
  
    if hasattr(self.machinetypes[machinetypeName], '_flavorID'):
      if self.machinetypes[machinetypeName]._flavorID:
        return self.machinetypes[machinetypeName]._flavorID
      else:
        raise OpenstackError('Flavor "' + self.machinetypes[machinetypeName].flavor_name + '" for machinetype ' + machinetypeName + ' not available!')
      
    try:
      result = self.httpRequest(self.computeURL + '/flavors/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
    
    try:
      for flavor in result['response']['flavors']:
        if flavor['name'] == self.machinetypes[machinetypeName].flavor_name:

          self.machinetypes[machinetypeName]._flavorID = flavor['id']

          try:
            # Record if available
            self.machinetypes[machinetypeName].mb = int(flavor['ram'])
          except:
            pass
            
          try:
            # Record if available
            self.machinetypes[machinetypeName].cpus = int(flavor['vcpus'])
          except:
            pass
            
          return self.machinetypes[machinetypeName]._flavorID
    except:
      pass
        
    raise OpenstackError('Flavor "' + self.machinetypes[machinetypeName].flavor_name + '" for machinetype ' + machinetypeName + ' not available!')

  def getImageID(self, machinetypeName):
    """Get the image ID"""

    # If we already know the image ID, then just return it
    if hasattr(self.machinetypes[machinetypeName], '_imageID'):
      if self.machinetypes[machinetypeName]._imageID:
        return self.machinetypes[machinetypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise OpenstackError('Image "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' not available!')

    # Get the existing images for this tenancy
    try:
      result = self.httpRequest(self.computeURL + '/images/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    # Specific image, not managed by Vcycle, lookup ID
    if self.machinetypes[machinetypeName].root_image[:6] == 'image:':
      for image in result['response']['images']:
         if self.machinetypes[machinetypeName].root_image[6:] == image['name']:
           self.machinetypes[machinetypeName]._imageID = str(image['id'])
           return self.machinetypes[machinetypeName]._imageID

      raise OpenstackError('Image "' + self.machinetypes[machinetypeName].root_image[6:] + '" for machinetype ' + machinetypeName + ' not available!')

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
          raise OpenstackError('Failed fetching ' + self.machinetypes[machinetypeName].root_image + ' (' + str(e) + ')')

        self.machinetypes[machinetypeName]._imageFile = imageFile
 
      elif self.machinetypes[machinetypeName].root_image[0] == '/':
        
        try:
          imageLastModified = int(os.stat(self.machinetypes[machinetypeName].root_image).st_mtime)
        except Exception as e:
          raise OpenstackError('Image file "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' does not exist!')

        self.machinetypes[machinetypeName]._imageFile = self.machinetypes[machinetypeName].root_image

      else: # root_image is not an absolute path, but imageName is
        
        try:
          imageLastModified = int(os.stat(imageName).st_mtime)
        except Exception as e:
          raise OpenstackError('Image file "' + self.machinetypes[machinetypeName].root_image +
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
        raise OpenstackError('Failed to verify signature/cert for ' + self.machinetypes[machinetypeName].root_image)
      elif re.search(self.machinetypes[machinetypeName].cernvm_signing_dn,  cernvmDict['dn']) is None:
        raise OpenstackError('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + self.machinetypes[machinetypeName].cernvm_signing_dn)
      else:
        vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

    # Try to upload the image
    try:
      self.machinetypes[machinetypeName]._imageID = self.uploadImage(self.machinetypes[machinetypeName]._imageFile, imageName, imageLastModified)
      return self.machinetypes[machinetypeName]._imageID
    except Exception as e:
      raise OpenstackError('Failed to upload image file ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageLastModified, verbose = False):

    try:
      f = open(imageFile, 'r')
    except Exception as e:
      raise OpenstackError('Failed to open image file ' + imageName + ' (' + str(e) + ')')

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
      raise OpenstackError('Failed uploadimg image to ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise OpenstackError('Upload to ' + url + ' returns HTTP error code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      response = json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise OpenstackError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')
    
    try:
      vcycle.vacutils.logLine('Uploaded new image ' + imageName + ' with ID ' + str(response['image']['id']))
      return str(response['image']['id'])
    except:
      raise OpenstackError('Failed to upload image file for ' + imageName + ' (' + str(e) + ')')

  def getKeyPairName(self, machinetypeName):
    """Get the key pair name from root_public_key"""

    if hasattr(self.machinetypes[machinetypeName], '_keyPairName'):
      if self.machinetypes[machinetypeName]._keyPairName:
        return self.machinetypes[machinetypeName]._keyPairName
      else:
        raise OpenstackError('Key pair "' + self.machinetypes[machinetypeName].root_public_key + '" for machinetype ' + machinetypeName + ' not available!')
      
    # Get the ssh public key from the root_public_key file
        
    if self.machinetypes[machinetypeName].root_public_key[0] == '/':
      try:
        f = open(self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        OpenstackError('Cannot open ' + self.machinetypes[machinetypeName].root_public_key)
    else:  
      try:
        f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key, 'r')
      except Exception as e:
        OpenstackError('Cannot open /var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + self.machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_public_key)

    while True:
      try:
        line = f.read()
      except:
        raise OpenstackError('Cannot find ssh-rsa public key line in ' + self.machinetypes[machinetypeName].root_public_key)
        
      if line[:8] == 'ssh-rsa ':
        sshPublicKey =  line.split(' ')[1]
        break

    # Check if public key is there already

    try:
      result = self.httpRequest(self.computeURL + '/os-keypairs',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

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
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created key pair ' + keyName + ' for ' + self.machinetypes[machinetypeName].root_public_key + ' in ' + self.spaceName)

    self.machinetypes[machinetypeName]._keyPairName = keyName
    return self.machinetypes[machinetypeName]._keyPairName

  def createMachine(self, machineName, machinetypeName):

    # OpenStack-specific machine creation steps

    try:
      if self.machinetypes[machinetypeName].remote_joboutputs_url:
        joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
      else:
        joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'
    
      request = { 'server' : 
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(machinetypeName),
                    'flavorRef' : self.getFlavorID(machinetypeName),
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

      if self.machinetypes[machinetypeName].root_public_key:
        request['server']['key_name'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise OpenstackError('Failed to create new machine: ' + str(e))

    try:
      result = self.httpRequest(self.computeURL + '/servers',
                                jsonRequest = request,
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + str(result['response']['server']['id']) + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
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
      self.httpRequest(self.computeURL + '/servers/' + self.machines[machineName].uuidStr,
                       method = 'DELETE',
                       headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')
