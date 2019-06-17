#!/usr/bin/python
#
#  google_api.py - a Google plugin for Vcycle
#
#  THIS FILE HAS BEEN UPDATED FOR Vcycle 3.0 BUT NEEDS VALIDATING
#  USING THE GOOGLE CLOUD SERVICE
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-9. All rights reserved.
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

  def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
  # Initialize data structures from configuration files

    # Generic initialization
    vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    # Google-specific initialization

    # Always has to be an explicit maximum, so default 1 if not given in [space ...] or [machinetype ...]
    if self.processors_limit is None:
      self.processors_limit = 1

    try:
      self.project_id = parser.get(spaceSectionName, 'project_id')
    except Exception as e:
      raise GoogleError('project_id is required in Google [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.zones = parser.get(spaceSectionName, 'zones').split()
    except Exception as e:
      raise GoogleError('The zones option is required in Google [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.client_email = parser.get(spaceSectionName, 'client_email')
    except Exception as e:
      raise GoogleError('The client_email option is required in Google [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.private_key = parser.get(spaceSectionName, 'private_key')
    except Exception as e:
      raise GoogleError('The private_key option is required in Google [space ' + spaceName + '] (' + str(e) + ')')

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

  def connect(self):
    """Connect to Google Compute Engine"""

    self.accessToken = self._getAccessToken()
    vcycle.vacutils.logLine('Connected to Google Compute Engine for space ' + self.spaceName)

    try:
      result = self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/global/images' % self.project_id,
                                headers = [ 'Authorization: Bearer ' + self.accessToken ])
    except Exception as e:
      raise GoogleError('Cannot connect to https://www.googleapis.com/compute/v1/projects/%s/global/images (%s)' % (self.project_id, str(e)))

    if 'items' in result['response']:
      self.images = result['response']['items']
    else:
      self.images = []

    for machinetypeName in self.machinetypes:
      try:
        self.machinetypes[machinetypeName].min_processors = self._googleMachineTypeProcessors(self.machinetypes[machinetypeName].flavor_names[0])
      except:
        pass

  def _googleMachineTypeProcessors(self, googleMachineType):
    # Return the number of processors associated with this Google MachineType
    # or 1 if we are unable to determine it

    shortMachineType = googleMachineType.split('/')[-1]

    if shortMachineType.startswith('custom-'):
      # custom-PROCESSORS-MEMORY
      try:
        return int(shortMachineType.split('-')[1])
      except:
        return 1

    if shortMachineType.startswith('n1-'):
      # n1-MEMORYNAME-PROCESSORS
      try:
        return int(shortMachineType.split('-')[2])
      except:
        return 1

    # Something else. Micro, small etc? Something new? Default to 1
    return 1

  def scanMachines(self):
    """Query Google Compute Engine for details of machines in this space"""

    # For each machine found in the space, this method is responsible for
    # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
    # or (b) creating a Machine object for the VM in self.spaces

    try:
      result = self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/aggregated/instances' % self.project_id,
                                headers = [ 'Authorization: Bearer ' + self.accessToken ])
    except Exception as e:
      raise GoogleError('Cannot get instances list (' + str(e) + ')')

    # Convert machines from None to an empty dictionary since we successfully connected
    self.machines = {}

    for oneZone in result['response']['items']:

      if 'instances' not in result['response']['items'][oneZone]:
        continue

      if oneZone.startswith('zones'):
        zone = oneZone[6:]
      else:
        zone = oneZone

      for oneMachine in result['response']['items'][oneZone]['instances']:
        machineName = str(oneMachine['name'])

        try:
          processors = self._googleMachineTypeProcessors(oneMachine['machineType'])

          # Just in case other VMs are in this space
          if not machineName.startswith('vcycle-'):
            # Still count VMs that we didn't create and won't manage, to avoid going above space limit
            self.totalProcessors += processors
            continue

          id = str(oneMachine['id'])

          # Try to get the IP address. Always use the zeroth member of the earliest network
          try:
            ip = str(oneMachine['networkInterfaces'][0]['accessConfigs'][0]['natIP'])
          except:
            ip = '0.0.0.0'

          # With GCE we only have createdTime. Machine class infers updatedTime and stoppedTime
          try:
            createdTime = calendar.timegm(time.strptime(str(oneMachine['creationTimestamp']), "%Y-%m-%dT%H:%M:%S-"))
          except:
            createdTime = None

          status = str(oneMachine['status'])

          try:
            machinetypeName = str(oneServer['metadata']['machinetype'])
          except:
            machinetypeName = None

          if status == 'RUNNING':
            state = vcycle.MachineState.running
          elif status == 'TERMINATED' or status == 'SUSPENDED':
            state = vcycle.MachineState.shutdown
          elif status == 'PROVISIONING' or status == 'STAGING':
            state = vcycle.MachineState.starting
          elif status == 'STOPPING' or status == 'SUSPENDING':
            state = vcycle.MachineState.deleting
          # No need to use vcycle.MachineState.failed? Covered by TERMINATED?
          else:
            state = vcycle.MachineState.unknown

          self.machines[machineName] = vcycle.shared.Machine(name             = machineName,
                                                             spaceName        = self.spaceName,
                                                             state            = state,
                                                             ip               = ip,
                                                             createdTime      = createdTime,
                                                             startedTime      = None,
                                                             updatedTime      = None,
                                                             uuidStr          = id,
                                                             machinetypeName  = machinetypeName,
                                                             zone             = zone)
        except Exception as e:
          vcycle.vacutils.logLine('Problem processing %s - skipping (%s)' % (machineName, str(e)))

  def _imageNameExists(self, imageName):
    """Check that imageName has already been uploaded to Google"""

    for imageDict in self.images:
      if 'name' in imageDict and imageDict['name'] == imageName:
        return True

    return False

  def _getImageName(self, machinetypeName):
    """Get the image Name"""

    # Existing image, not managed by Vcycle. Easy.
    if self.machinetypes[machinetypeName].root_image.startswith('image:'):
      imageName = self.machinetypes[machinetypeName].root_image[6:]
      if not self._imageNameExists(imageName):
        raise GoogleError('%s is not an existing image on GCE!' % imageName)

      return imageName

# Just image: for now.
    raise GoogleError('Only existing images are supported! Please use root_image = image:IMAGENAME')

# To enable the rest of this method, self.uploadImage() must first be updated for GCE (it's still OS.)

    # imageFile is the full path to the source file or cached file on disk on this VM factory machine
    # imageURL is the remote URL or local source path of the image, to use in the hashes given to GCE
    # imageName is the hash of the modification time and the URL/path of the image: new for each version
    # imageFamily is the hash of just the URL or file path of the image: it doesn't change as the version
    #   changes and it can be used to identify old versions which should be deleted

    if self.machinetypes[machinetypeName].root_image.startswith('http://') or \
       self.machinetypes[machinetypeName].root_image.startswith('https://'):

      try:
          imageFile = vcycle.vacutils.getRemoteRootImage(self.machinetypes[machinetypeName].root_image,
                                         '/var/lib/vcycle/imagecache',
                                         '/var/lib/vcycle/tmp',
                                         'Vcycle ' + vcycle.shared.vcycleVersion)

          imageLastModified = int(os.stat(imageFile).st_mtime)
          imageURL = self.machinetypes[machinetypeName].root_image

      except Exception as e:
          raise GoogleError('Failed fetching ' + self.machinetypes[machinetypeName].root_image + ' (' + str(e) + ')')

    else:
      if self.machinetypes[machinetypeName].root_image[0] == '/':
          imageFile = self.machinetypes[machinetypeName].root_image
      else:
          imageFile = '/var/lib/vcycle/spaces/' + self.spaceName + '/machinetypes/' + machinetypeName + '/files/' + self.machinetypes[machinetypeName].root_image

      try:
          imageLastModified = int(os.stat(imageFile).st_mtime)
      except Exception as e:
          raise GoogleError('Image file "' + imageFile + '" does not exist!')

      imageURL = imageFile

    # Create the hash for use with the GCE store of images
    imageName = base64.b32encode(hashlib.sha256(str(imageLastModified) + ' ' + imageURL).digest()).lower().replace('=','0')

    if self._imageNameExists(imageName):
      # We already have it!
      return imageName

    vcycle.vacutils.logLine('Image "' + self.machinetypes[machinetypeName].root_image + '" not found in GCE image store, so need to upload')

    # Create the family name, which is also a hash but doesn't change with new versions
    imageFamily = base64.b32encode(hashlib.sha256(imageURL).digest()).lower().replace('=','0')

    if self.machinetypes[machinetypeName].cernvm_signing_dn:
      cernvmDict = vac.vacutils.getCernvmImageData(imageFile)

      if cernvmDict['verified'] == False:
        raise GoogleError('Failed to verify signature/cert for ' + self.machinetypes[machinetypeName].root_image)
      elif re.search(self.machinetypes[machinetypeName].cernvm_signing_dn,  cernvmDict['dn']) is None:
        raise GoogleError('Signing DN ' + cernvmDict['dn'] + ' does not match cernvm_signing_dn = ' + self.machinetypes[machinetypeName].cernvm_signing_dn)
      else:
        vac.vacutils.logLine('Verified image signed by ' + cernvmDict['dn'])

    # Try to upload the image
    try:
      self.uploadImage(imageFile, imageName, imageFamily)
      return imageName

    except Exception as e:
      raise GoogleError('Failed to upload image file as ' + imageName + ' (' + str(e) + ')')

  def uploadImage(self, imageFile, imageName, imageFamily, verbose = False):
#
# This is the OpenStack version! We need to write a GCE version. It will look similar :)
#
# It needs to implement something like this using the REST API:
#   gsutil mb gs://vcycle
#   gsutil cp cernvm3-micro-2.7-7.tar.gz gs://vcycle/cernvm3-micro-2.7-7.tar.gz
#   gcloud compute images create cernvm3 --source-uri=gs://vcycle/cernvm3-micro-2.7-7.tar.gz
#
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

  def _cvmUserData(self, machinetypeName):
    # Create a user-data file for use with amiconfig in CernVM 3, which looks for the
    # metadata key cvm-user-data. The sed commands make the EC2 support in Cloud Init
    # find the user-data file in the GCE metadata, when it is run after amiconfig.

    template = """#! /bin/bash
#
# Hotfix EC2 paths to work with GCE !!!
#
sed -i 's:^DEF_MD_VERSION.*:DEF_MD_VERSION = "0.1":' \
  /usr/lib/python2.6/site-packages/cloudinit/sources/DataSourceEc2.py
sed -i 's:ec2.get_instance_userdata.self.api_ver,.*:ec2.get_instance_userdata\(self.api_ver + "/meta-data/attributes",:' \
  /usr/lib/python2.6/site-packages/cloudinit/sources/DataSourceEc2.py
sed -i 's:\[NoCloud:[Ec2, NoCloud:' /etc/cloud/cloud.cfg.d/50_cernvm.cfg
exit 0
[amiconfig]
plugins=cernvm
[cernvm]
repositories=grid
proxy=##user_data_option_cvmfs_proxy##
[ucernvm-begin]
resize_rootfs=off
cvmfs_http_proxy='##user_data_option_cvmfs_proxy##'
[ucernvm-end]
"""

    try:
      proxyExpr = self.machinetypes[machinetypeName].options['user_data_cvmfs_proxy']
    except:
      proxyExpr = 'DIRECT'

    return base64.b64encode(template.replace('##user_data_option_cvmfs_proxy##', proxyExpr))

  def createMachine(self, machineName, machinetypeName, zone):
    # Google-specific machine creation steps (included hardcoded default 40 GB/processor if not explicitly given)

    try:
      userData = self.getFileContents(machineName, 'user_data')

      if self.machinetypes[machinetypeName].disk_gb_per_processor is None:
        disk_gb_per_processor = 40
      else:
        disk_gb_per_processor = self.machinetypes[machinetypeName].disk_gb_per_processor

      request = { 'name'        : machineName,
                  'machineType' : 'zones/%s/machineTypes/%s' % (zone, self.machinetypes[machinetypeName].flavor_names[0]),
                  'disks' : [
                              {
                                'initializeParams' : { 'diskSizeGb'  : disk_gb_per_processor * self.machinetypes[machinetypeName].min_processors,
                                                       'sourceImage' : 'global/images/' + self._getImageName(machinetypeName) },
                                'boot'             : True,
                                'autoDelete'       : True
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
                                           { 'key'   : 'user-data',
                                             'value' :  userData },
                                           { 'key'   : 'machinetype',
                                             'value' :  machinetypeName },
                                           { 'key'   : 'machinefeatures',
                                             'value' : 'https://' + https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/machinefeatures' },
                                           { 'key'   : 'jobfeatures',
                                             'value' : 'https://' + https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/jobfeatures' },
                                           { 'key'   : 'joboutputs',
                                             'value' : 'https://' + https_host + ':' + str(self.https_port) + '/machines/' + self.spaceName + '/' + machineName + '/joboutputs' }
                                         ]
                              }
                }

      if userData.startswith('From '):
        # user_data file looks like Cloud Init, so for CernVM 3 we add the amiconfig wrapper cvm-user-data file
        request['metadata']['items'].append( { 'key'   : 'cvm-user-data',
                                               'value' : self._cvmUserData(machinetypeName) } )

      # Get the ssh public key from the root_public_key file
      if self.machinetypes[machinetypeName].root_public_key:
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
            f.close()
            raise GoogleError('Cannot find ssh-rsa public key line in ' + self.machinetypes[machinetypeName].root_public_key)

          if line.startswith('ssh-rsa '):
            f.close()
            sshPublicKey = line.split(' ')[1]
            break

        # Only get this far if an "ssh-rsa ..." line has been found

        # This old version is still needed by google daemon inside CernVM 3
        request['metadata']['items'].append( { 'key' : 'sshKeys',
                                               'value' : 'root:ssh-rsa ' + sshPublicKey + ' root' } )

        # This is the current version
        request['metadata']['items'].append( { 'key' : 'ssh-keys',
                                               'value' : 'root:ssh-rsa ' + sshPublicKey + ' root' } )

    except Exception as e:
      raise GoogleError('Failed to create new machine request for %s: %s' % (machineName, str(e)))

    try:
      result = self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances' % (self.project_id, zone),
                                jsonRequest = request,
                                headers = [ 'Authorization: Bearer ' + self.accessToken ])
    except Exception as e:
      raise GoogleError('Cannot create VM (' + str(e) + ')')

    try:
      uuidStr = str(result['response']['id'])
    except:
      raise GoogleError('Unable to get VM id from GCE instance insert response (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + uuidStr + ') for ' + machinetypeName + ' within ' + self.spaceName)

    self.machines[machineName] = vcycle.shared.Machine(name            = machineName,
                                                       spaceName       = self.spaceName,
                                                       state           = vcycle.MachineState.starting,
                                                       ip              = '0.0.0.0',
                                                       createdTime     = int(time.time()),
                                                       startedTime     = None,
                                                       updatedTime     = int(time.time()),
                                                       uuidStr         = uuidStr,
                                                       machinetypeName = machinetypeName,
                                                       zone            = zone)

  def deleteOneMachine(self, machineName):

    try:
      self.httpRequest('https://www.googleapis.com/compute/v1/projects/%s/zones/%s/instances/%s' % (self.project_id, self.machines[machineName].zone, machineName),
                       method = 'DELETE',
                       headers = [ 'Authorization: Bearer ' + self.accessToken ])

    except Exception as e:
      raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' (' + str(e) + ')')
