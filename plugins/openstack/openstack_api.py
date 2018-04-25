#!/usr/bin/python
#
#  openstack_api.py - an OpenStack plugin for Vcycle
#
#  Andrew McNab, Raoul Hidalgo Charman,
#  University of Manchester.
#  Copyright (c) 2013-8. All rights reserved.
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

from vcycle.core import shared
from vcycle.core import vacutils
from vcycle.plugins import openstack

class OpenstackError(Exception):
  pass

class OpenstackSpace(shared.BaseSpace):

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
    # Initialize data structures from configuration files
    # Generic initialization
    shared.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    # OpenStack-specific initialization
    try:
      self.project_name = parser.get(spaceSectionName, 'tenancy_name')
    except:
      try:
        self.project_name = parser.get(spaceSectionName, 'project_name')
      except Exception as e:
        raise OpenstackError('project_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')
    else:
      vacutils.logLine('tenancy_name in [space ' + self.spaceName + '] is deprecated - please use project_name')

    try:
      self.domain_name = parser.get(spaceSectionName, 'domain_name')
    except Exception as e:
      self.domain_name = 'default'

    try:
      self.network_uuid = parser.get(spaceSectionName, 'network_uuid')
    except Exception as e:
      self.network_uuid = None

    try:
      self.region = parser.get(spaceSectionName, 'region')
    except Exception as e:
      self.region = None

    try:
      self.zones = parser.get(spaceSectionName, 'zones').split()
    except Exception as e:
      self.zones = None

    try:
      self.identityURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise OpenstackError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.glanceAPIVersion = parser.get(spaceSectionName, 'glance_api')
    except Exception as e:
      raise OpenstackError('glance_api is required in OpenStack [space '
          + spaceName + '] (' + str(e) + ')')

    try:
      self.osShutdownTimeout = parser.get(spaceSectionName, 'os_shutdown_timeout')
    except:
      self.osShutdownTimeout = None

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

    if self.apiVersion and self.apiVersion != '2' and not self.apiVersion.startswith('2.') and self.apiVersion != '3' and not self.apiVersion.startswith('3.'):
      raise OpenstackError('api_version %s not recognised' % self.apiVersion)

  def connect(self):
  # Wrapper around the connect methods and some common post-connection updates

    if not self.apiVersion or self.apiVersion == '2' or self.apiVersion.startswith('2.'):
      self._connectV2()
    elif self.apiVersion == '3' or self.apiVersion.startswith('3.'):
      self._connectV3()
    else:
      # This rechecks the checking done in the constructor called by readConf()
      raise OpenstackError('api_version %s not recognised' % self.apiVersion)

    # initialise glance api (has to be here as we don't have imageURL until
    # after connecting)
    if self.glanceAPIVersion == '2':
      self.imageAPI = openstack.image_api.GlanceV2(self.token, self.imageURL)
      # update image additional properties
      self.imageAPI.updateShutdownTimeout(self.osShutdownTimeout)
    elif self.glanceAPIVersion == '1':
      self.imageAPI = openstack.image_api.GlanceV1(self.token, self.imageURL)
    else:
      raise OpenstackError('glanceAPIVersion %s not recongnised'
          % self.glanceAPIVersion)

    # Build dictionary of flavor details using API
    self._getFlavors()

    # Try to get the limit on the number of processors in this project
    processorsLimit =  self._getProcessorsLimit()

    # Try to use it for this space
    if self.processors_limit is None:
      vacutils.logLine('No limit on processors set in Vcycle configuration')
      if processorsLimit is not None:
        vacutils.logLine('Processors limit set to %d from OpenStack' % processorsLimit)
        self.processors_limit = processorsLimit
    else:
      vacutils.logLine('Processors limit set to %d in Vcycle configuration' % self.processors_limit)

  def _connectV2(self):
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

    vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vacutils.logLine('computeURL = ' + self.computeURL)
    vacutils.logLine('imageURL   = ' + self.imageURL)

  def _connectV3(self):
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

    # This might be a bit naive? We just keep the LAST matching one we see.
    for service in result['response']['token']['catalog']:

      if service['type'] == 'compute':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public' and \
              (self.region is None or self.region == endpoint['region']):
            self.computeURL = str(endpoint['url'])

      elif service['type'] == 'image':
        for endpoint in service['endpoints']:
          if endpoint['interface'] == 'public' and \
              (self.region is None or self.region == endpoint['region']):
            self.imageURL = str(endpoint['url'])

    if not self.computeURL:
      raise OpenstackError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise OpenstackError('No image service URL found from ' + self.identityURL)

    vacutils.logLine('Connected to ' + self.identityURL + ' for space ' + self.spaceName)
    vacutils.logLine('computeURL = ' + self.computeURL)
    vacutils.logLine('imageURL   = ' + self.imageURL)

  def _getFlavors(self):
    """Query OpenStack to get details of flavors defined for this project"""

    self.flavors = {}

    try:
      result = self.httpRequest(self.computeURL + '/flavors/detail',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneFlavor in result['response']['flavors']:

      flavor = {}
      flavor['mb']          = oneFlavor['ram']
      flavor['processors']  = oneFlavor['vcpus']
      flavor['id']          = oneFlavor['id']

      self.flavors[oneFlavor['name']] = flavor

  def _getProcessorsLimit(self):
    """Query OpenStack to get processor limit for this project"""

    try:
      result = self.httpRequest(self.computeURL + '/limits',
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    try:
      return int(result['response']['limits']['absolute']['maxTotalCores'])
    except:
      return None

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

      try:
        flavorID = oneServer['flavor']['id']
      except:
        flavorID   = None
        processors = 1
      else:
        try:
          processors = self.flavors[self.getFlavorName(flavorID)]['processors']
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
      else:
        if machinetypeName not in self.machinetypes:
          machinetypeName = None

      try:
        zone = str(oneServer['OS-EXT-AZ:availability_zone'])
      except:
        zone = None

      if taskState == 'Deleting':
        state = shared.MachineState.deleting
      elif status == 'ACTIVE' and powerState == 1:
        state = shared.MachineState.running
      elif status == 'BUILD' or status == 'ACTIVE':
        state = shared.MachineState.starting
      elif status == 'SHUTOFF':
        state = shared.MachineState.shutdown
      elif status == 'ERROR':
        state = shared.MachineState.failed
      elif status == 'DELETED':
        state = shared.MachineState.deleting
      else:
        state = shared.MachineState.unknown

      self.machines[machineName] = shared.Machine(
          name             = machineName,
          spaceName        = self.spaceName,
          state            = state,
          ip               = ip,
          createdTime      = createdTime,
          startedTime      = startedTime,
          updatedTime      = updatedTime,
          uuidStr          = uuidStr,
          machinetypeName  = machinetypeName,
          zone             = zone,
          processors       = processors)

  def getFlavorName(self, flavorID):
    """Get the "flavor" ID"""

    for flavorName in self.flavors:
      if self.flavors[flavorName]['id'] == flavorID:
        return flavorName

    raise OpenstackError('Flavor "' + flavorID + '" not available!')

  def getImageID(self, machinetypeName):
    """ Get the image ID """

    # If we already know the image ID, then just return it
    if hasattr(self.machinetypes[machinetypeName], '_imageID'):
      if self.machinetypes[machinetypeName]._imageID:
        return self.machinetypes[machinetypeName]._imageID
      else:
        # If _imageID is None, then it's not available for this cycle
        raise OpenstackError('Image "' + self.machinetypes[machinetypeName].root_image + '" for machinetype ' + machinetypeName + ' not available!')

    # Get the existing images for this tenancy
    result = self.imageAPI.getImageDetails()

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
          imageFile = vacutils.getRemoteRootImage(self.machinetypes[machinetypeName].root_image,
                                         '/var/lib/vcycle/imagecache',
                                         '/var/lib/vcycle/tmp',
                                         'Vcycle ' + shared.vcycleVersion)

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
    # Glance v2 api differs by keeping metadata in tags
    if self.glanceAPIVersion == '1':
      for image in result['response']['images']:
        try:
          if image['name'] == imageName and \
              image['status'] == 'ACTIVE' and \
              image['metadata']['last_modified'] == str(imageLastModified):
            self.machinetypes[machinetypeName]._imageID = str(image['id'])
            return self.machinetypes[machinetypeName]._imageID
        except:
          pass
    elif self.glanceAPIVersion == '2':
      for image in result['response']['images']:
        try:
          if image['name'] == imageName and image['status'] == 'active':
            for tag in image['tags']:
              if tag.lstrip('last_modified: ') == str(imageLastModified):
                self.machinetypes[machinetypeName]._imageID = str(image['id'])
                return self.machinetypes[machinetypeName]._imageID
        except:
          pass

    vacutils.logLine('Image "' + self.machinetypes[machinetypeName].root_image + '" not found in image service, so uploading')

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

  def uploadImage(self, imageFile, imageName, imageLastModified,
                  verbose = False):
    return self.imageAPI.uploadImage(imageFile, imageName, imageLastModified,
                                     verbose)

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

    vacutils.logLine('Created key pair ' + keyName + ' for ' + self.machinetypes[machinetypeName].root_public_key + ' in ' + self.spaceName)

    self.machinetypes[machinetypeName]._keyPairName = keyName
    return self.machinetypes[machinetypeName]._keyPairName

  def createMachine(self, machineName, machinetypeName, zone = None):
    # OpenStack-specific machine creation steps
    
    # Find the first flavor matching min_processors:max_processors
    flavorName = None
    
    for fn in self.machinetypes[machinetypeName].flavor_names:
      if fn in self.flavors:
        if self.machinetypes[machinetypeName].min_processors <= self.flavors[fn]['processors'] and \
           (self.machinetypes[machinetypeName].max_processors is None or \
            self.machinetypes[machinetypeName].max_processors >= self.flavors[fn]['processors']):
          flavorName = fn
          break
    
    if not flavorName:
      raise OpenstackError('No flavor suitable for machinetype ' + machinetypeName)

    try:
      if self.machinetypes[machinetypeName].remote_joboutputs_url:
        joboutputsURL = self.machinetypes[machinetypeName].remote_joboutputs_url + machineName
      else:
        joboutputsURL = 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/joboutputs'

      request = { 'server' :
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(machinetypeName),
                    'flavorRef' : self.flavors[flavorName]['id'],
                    'metadata'  : { 'cern-services'   : 'false',
                                    'name'	      : machineName,
                                    'machinetype'     : machinetypeName,
                                    'machinefeatures' : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/machinefeatures',
                                    'jobfeatures'     : 'https://' + os.uname()[1] + ':' + str(self.https_port) + '/machines/' + machineName + '/jobfeatures',
                                    'joboutputs'      : joboutputsURL  }
                  }
                }

      if self.network_uuid:
        request['server']['networks'] = [{"uuid": self.network_uuid}]
        vacutils.logLine('Will use network %s for %s' % (self.network_uuid, machineName))

      if zone:
        request['server']['availability_zone'] = zone
        vacutils.logLine('Will request %s be created in zone %s of space %s' % (machineName, zone, self.spaceName))

      if self.machinetypes[machinetypeName].root_public_key:
        request['server']['key_name'] = self.getKeyPairName(machinetypeName)

    except Exception as e:
      raise OpenstackError('Failed to create new machine %s: %s' % (machineName, str(e)))

    try:
      result = self.httpRequest(self.computeURL + '/servers',
                                jsonRequest = request,
                                headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise OpenstackError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    try:
      uuidStr = str(result['response']['server']['id'])
    except:
      raise OpenstackError('Could not get VM UUID from VM creation response (' + str(e) + ')')

    vacutils.logLine('Created ' + machineName + ' (' + uuidStr + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = shared.Machine(name             = machineName,
                                                       spaceName        = self.spaceName,
                                                       state            = shared.MachineState.starting,
                                                       ip               = '0.0.0.0',
                                                       createdTime      = int(time.time()),
                                                       startedTime      = None,
                                                       updatedTime      = int(time.time()),
                                                       uuidStr          = uuidStr,
                                                       machinetypeName  = machinetypeName,
                                                       processors       = self.flavors[flavorName]['processors'])

  def deleteOneMachine(self, machineName):

    try:
      self.httpRequest(self.computeURL + '/servers/' + self.machines[machineName].uuidStr,
                       method = 'DELETE',
                       headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise shared.VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')
