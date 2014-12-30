#!/usr/bin/python
#
#  shared.py - common functions, classes, and variables for Vcycle
#
#  Andrew McNab, University of Manchester.
#  Copyright (c) 2013-4. All rights reserved.
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
import string
import pycurl
import random
import base64
import StringIO
import tempfile
import calendar
import ConfigParser

import vcycle.vacutils

vcycleVersion = None
spaces        = None

class MachineState:
   unknown, shutdown, starting, running, deleting, failed = ('Unknown', 'Shut down', 'Starting', 'Running', 'Deleting', 'Failed')
   
class Machine:

  def __init__(self, name, state, ip, createdTime, startedTime, updatedTime):

    # Store values from api-specific calling function
    self.name         = name
    self.state        = state
    self.ip           = ip
    self.createdTime  = createdTime
    self.startedTime  = startedTime
    self.updatedTime  = updatedTime
    
    # Record when the machine started (rather than just being created)
    if startedTime and not os.path.isfile('/var/lib/vcycle/machines/' + name + '/started'):
      vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + name + '/started', str(startedTime), 0600, '/var/lib/vcycle/tmp')

    # Store values stored when we requested the machine
    try:
      f = open('/var/lib/vcycle/machines/' + name + '/space_name', 'r')
    except:
      self.spaceName = None
    else:
      self.spaceName = f.read().strip()
      f.close()

    try:
      f = open('/var/lib/vcycle/machines/' + name + '/vmtype', 'r')
    except:
      self.vmtypeName = None
    else:
      self.vmtypeName = f.read().strip()
      f.close()

    try:
      spaces[self.spaceName].totalMachines += 1
      spaces[self.spaceName].vmtypes[self.vmtypeName].totalMachines += 1

      if spaces[self.spaceName].vmtypes[self.vmtypeName].target_share > 0.0:
        spaces[self.spaceName].vmtypes[self.vmtypeName].weightedMachines += (1.0 / spaces[self.spaceName].vmtypes[self.vmtypeName].target_share)
    except:
      pass
      
    if self.state == MachineState.running:
      try:
        spaces[self.spaceName].runningMachines += 1
        spaces[self.spaceName].vmtypes[self.vmtypeName].runningMachines += 1
      except:
        pass

    try:        
      if (self.state == starting or self.state == running) and \
         ((int(time.time()) - startTime) < spaces[self.spaceName].vmtypes[self.vmtypeName].fizzle_seconds):
        spaces[self.spaceName].vmtypes[self.vmtypeName].notPassedFizzle += 1
    except:
      pass

    # Possibly created by the machine itself
    try:
      self.heartbeatTime = int(os.stat('/var/lib/vcycle/machines/' + name + '/machineoutputs/vm-heartbeat').st_ctime)
    except:
      self.heartbeatTime = None

    vcycle.vacutils.logLine(name + ' in ' + 
                            str(self.spaceName) + ':' +
                            str(self.vmtypeName) + ' ' + 
                            self.ip + ' ' + 
                            self.state + ' ' + 
                            str(self.createdTime) + '-' +
                            str(self.startedTime) + '-' +
                            str(self.updatedTime) + ' ' +
                            str(self.heartbeatTime))

class Vmtype:

  def __init__(self, spaceName, vmtypeName, parser, vmtypeSectionName):
  
    self.spaceName  = spaceName
    self.vmtypeName = vmtypeName

    # Recreate lastFizzleTime (must be set/updated with setLastFizzleTime() to create file)
    try:
      f = open('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName + '/last_fizzle_time', 'r')
    except:
      self.lastFizzleTime = 0
    else:     
      self.lastFizzleTime = int(f.read().strip())
      f.close()
  
    try:
      self.root_image = parser.get(vmtypeSectionName, 'root_image')
    except Exception as e:
      raise NameError('root_image is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.flavor_name = parser.get(vmtypeSectionName, 'flavor_name')
    except Exception as e:
      raise NameError('flavor_name is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
    
    try:
      self.root_public_key = parser.get(vmtypeSectionName, 'root_public_key')
    except:
      self.root_public_key = None
    
    try:
      self.x509dn = parser.get(vmtypeSectionName, 'x509dn')
    except:
      self.x509dn = None
    
    try:
      if parser.has_option(vmtypeSectionName, 'max_machines'):
        self.max_machines = int(parser.get(vmtypeSectionName, 'max_machines'))
      else:
        self.max_machines = None
    except Exception as e:
      raise NameError('Failed to parse max_machines in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.backoff_seconds = int(parser.get(vmtypeSectionName, 'backoff_seconds'))
    except Exception as e:
      raise NameError('backoff_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.fizzle_seconds = int(parser.get(vmtypeSectionName, 'fizzle_seconds'))
    except Exception as e:
      raise NameError('fizzle_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      if parser.has_option(vmtypeSectionName, 'max_wallclock_seconds'):
        self.max_wallclock_seconds = int(parser.get(vmtypeSectionName, 'max_wallclock_seconds'))
      else:
        self.max_wallclock_seconds = 86400
    except Exception as e:
      raise NameError('max_wallclock_seconds is required in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    try:
      self.heartbeat_file = parser.get(vmtypeSectionName, 'heartbeat_file')
    except:
      self.heartbeat_file = None

    try:
      if parser.has_option(vmtypeSectionName, 'heartbeat_seconds'):
        self.heartbeat_seconds = int(parser.get(vmtypeSectionName, 'heartbeat_seconds'))
      else:
        self.heartbeat_seconds = None
    except Exception as e:
      raise NameError('Failed to parse heartbeat_seconds in [' + vmtypeSectionName + '] (' + str(e) + ')')

    try:
      self.user_data = parser.get(vmtypeSectionName, 'user_data')
    except Exception as e:
      raise NameError('user_data is required in [' + vmtypeSectionName + '] (' + str(e) + ')')

    try:
      if parser.has_option(vmtypeSectionName, 'target_share'):
        self.target_share = float(parser.get(vmtypeSectionName, 'target_share'))
      else:
        self.target_share = 0.0
    except Exception as e:
      raise NameError('Failed to parse target_share in [' + vmtypeSectionName + '] (' + str(e) + ')')

    if parser.has_option(vmtypeSectionName, 'log_machineoutputs') and \
               parser.get(vmtypeSectionName, 'log_machineoutputs').strip().lower() == 'true':
      self.log_machineoutputs = True
    else:
      self.log_machineoutputs = False

    try:
      if parser.has_option(vmtypeSectionName, 'machineoutputs_days'):
        self.machineoutputs_days = float(parser.get(vmtypeSectionName, 'machineoutputs_days'))
      else:
        self.machineoutputs_days = 3.0
    except Exception as e:
      raise NameError('Failed to parse machineoutputs_days in [' + vmtypeSectionName + '] (' + str(e) + ')')
      
    self.options = {}
    
    for (oneOption, oneValue) in parser.items(vmtypeSectionName):
      if (oneOption[0:17] == 'user_data_option_') or (oneOption[0:15] == 'user_data_file_'):
        if string.translate(oneOption, None, '0123456789abcdefghijklmnopqrstuvwxyz_') != '':
          raise NameError('Name of user_data_xxx (' + oneOption + ') must only contain a-z 0-9 and _')
        else:
          self.options[oneOption] = oneValue

    if parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                not parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      raise NameError('user_data_proxy_cert given but user_data_proxy_key missing (they can point to the same file if necessary)')
    elif not parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      raise NameError('user_data_proxy_key given but user_data_proxy_cert missing (they can point to the same file if necessary)')
    elif parser.has_option(vmtypeSectionName, 'user_data_proxy_cert') and \
                  parser.has_option(vmtypeSectionName, 'user_data_proxy_key') :
      self.user_data_proxy_cert = parser.get(vmtypeSectionName, 'user_data_proxy_cert')
      self.user_data_proxy_key  = parser.get(vmtypeSectionName, 'user_data_proxy_key')
             
    if parser.has_option(vmtypeSectionName, 'legacy_proxy') and \
       parser.get(vmtypeSectionName, 'legacy_proxy').strip().lower() == 'true':
      self.legacy_proxy = True
    else:
      self.legacy_proxy = False

    # Just for this instance, so Total for this vmtype in one space
    self.totalMachines    = 0
    self.runningMachines  = 0.0
    self.weightedMachines = 0.0
    self.notPassedFizzle  = 0

  def setLastFizzleTime(self, fizzleTime):

    if fizzleTime > self.lastFizzleTime:
      self.lastFizzleTime = fizzleTime
      
      os.makedirs('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName,
                  stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
      vcycle.vacutils.createFile('/var/lib/vcycle/spaces/' + self.spaceName + '/' + self.vmtypeName, str(fizzleTime), tmpDir = '/var/lib/vcycle/tmp')

class BaseSpace:

  def __init__(self, api, spaceName, parser, spaceSectionName):
    self.api       = api
    self.spaceName = spaceName

    try:
      self.max_machines = int(parser.get(spaceSectionName, 'max_machines'))
    except Exception as e:
      raise NameError('max_machines is required in [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.delete_old_files = bool(parser.get(spaceSectionName, 'delete_old_files'))
    except:
      self.delete_old_files = True

    self.vmtypes = {}

    for vmtypeSectionName in parser.sections():
      try:
        (sectionType, spaceTemp, vmtypeName) = vmtypeSectionName.lower().split(None,2)
      except:
        continue

      if sectionType != 'vmtype' or spaceTemp != spaceName:
        continue

      if string.translate(vmtypeName, None, '0123456789abcdefghijklmnopqrstuvwxyz-') != '':
        raise NameError('Name of vmtype in [vmtype ' + spaceName + ' ' + vmtypeName + '] can only contain a-z 0-9 or -')

      try:
        self.vmtypes[vmtypeName] = Vmtype(spaceName, vmtypeName, parser, vmtypeSectionName)
      except Exception as e:
        raise NameError('Failed to initialize [vmtype ' + spaceName + ' ' + vmtypeName + '] (' + str(e) + ')')

    if len(self.vmtypes) < 1:
      raise NameError('No vmtypes defined for space ' + spaceName + ' - each space must have at least one vmtype!')

    # Start new curl session for this instance
    self.curl = pycurl.Curl()
    
    self.token = None

    # totalMachines includes ones Vcycle doesn't manage
    self.totalMachines   = 0
    self.runningMachines = 0
    
    # all the Vcycle-created VMs in this space
    self.machines = {}

  def httpJSON(self, url, request = None, headers = None):

#    print 'Start httpJSON',url,request,headers

    self.curl.setopt(pycurl.URL, str(url))

#    print 'after set url'

    if request:
      try:
        self.curl.setopt(pycurl.POSTFIELDS, json.dumps(request))
      except Exception as e:
        raise NameError('JSON encoding of "' + str(request) + '" fails (' + str(e) + ')')
    else:
      self.curl.setopt(pycurl.HTTPGET, True)

    outputBuffer = StringIO.StringIO()
    self.curl.setopt(pycurl.WRITEFUNCTION, outputBuffer.write)
    
    allHeaders = ['Content-Type: application/json', 'Accept: application/json']

    if headers:
      allHeaders.extend(headers)

    self.curl.setopt(pycurl.HTTPHEADER, allHeaders)

#    self.curl.setopt(pycurl.VERBOSE, 2)

    self.curl.setopt(pycurl.TIMEOUT, 30)
    self.curl.setopt(pycurl.FOLLOWLOCATION, False)
    self.curl.setopt(pycurl.SSL_VERIFYPEER, 1)
    self.curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        
    if os.path.isdir('/etc/grid-security/certificates'):
      self.curl.setopt(pycurl.CAPATH, '/etc/grid-security/certificates')

    try:
      self.curl.perform()
    except Exception as e:
      raise NameError('Failed to read ' + url + ' (' + str(e) + ')')

    # Any 2xx code is OK; otherwise raise an exception
    if self.curl.getinfo(pycurl.RESPONSE_CODE) / 100 != 2:
      raise NameError('Query of ' + url + ' returns HTTP code ' + str(self.curl.getinfo(pycurl.RESPONSE_CODE)))

    try:
      return json.loads(outputBuffer.getvalue())
    except Exception as e:
      raise NameError('JSON decoding of HTTP(S) response fails (' + str(e) + ')')

  def cleanupMachines(self):
  
    # setLastFizzleTime() here too
    print 'We do not actually clean up old machines yet ...'

  def makeMachines(self):

    vcycle.vacutils.logLine('Space ' + self.spaceName + 
                            ' has ' + str(self.runningMachines) + 
                            ' running vcycle VMs out of ' + str(self.totalMachines) +
                            ' found in any state for any vmtype or none')
  
    for vmtypeName,vmtype in self.vmtypes.iteritems():
      vcycle.vacutils.logLine('vmtype ' + vmtypeName + 
                              ' has ' + str(vmtype.runningMachines) + 
                              ' running vcycle VMs out of ' + str(vmtype.totalMachines) +
                              ' found in any state')
  
    creationsPerCycle  = int(0.9999999 + self.max_machines * 0.1)
    creationsThisCycle = 0

    # Keep making passes through the vmtypes until limits exhausted
    while True:
      if self.totalMachines >= self.max_machines:
        vcycle.vacutils.logLine('Reached limit (%d) on number of machines to create for space %s' % (self.max_machines, self.spaceName))
        return

      if creationsThisCycle >= creationsPerCycle:
        vcycle.vacutils.logLine('Already reached limit of %d machine creations this cycle' % creationsThisCycle )
        return
      
      # For each pass, vmtypes are visited in a random order
      vmtypeNames = self.vmtypes.keys()
      random.shuffle(vmtypeNames)

      # Will record the best vmtype to create
      bestVmtypeName = None

      for vmtypeName in vmtypeNames:
        if self.vmtypes[vmtypeName].target_share <= 0.0:
          continue

        if self.vmtypes[vmtypeName].totalMachines >= self.vmtypes[vmtypeName].max_machines:
          vcycle.vacutils.logLine('Reached limit (' + str(self.vmtypes[vmtypeName].totalMachines) + ') on number of machines to create for vmtype ' + vmtypeName)
          continue

        if int(time.time()) < (self.vmtypes[vmtypeName].lastFizzleTime + self.vmtypes[vmtypeName].backoff_seconds):
          vcycle.vacutils.logLine('Free capacity found for %s ... but only %d seconds after last fizzle' 
                                  % (vmtypeName, int(time.time()) - self.vmtypes[vmtypeName].lastFizzleTime) )
          continue

        if (int(time.time()) < (self.vmtypes[vmtypeName].lastFizzleTime + 
                                self.vmtypes[vmtypeName].backoff_seconds + 
                                self.vmtypes[vmtypeName].fizzle_seconds)) and \
           (self.vmtypes[vmtypeName].notPassedFizzle > 0):
          vcycle.vacutils.logLine('Free capacity found for ' + 
                                  vmtypeName + 
                                  ' ... but still within fizzle_seconds+backoff_seconds(' + 
                                  str(int(self.vmtypes[vmtypeName].backoff_seconds + self.vmtypes[vmtypeName].fizzle_seconds)) + 
                                  ') of last fizzle (' + 
                                  str(int(time.time()) - self.vmtypes[vmtypeName].lastFizzleTime) + 
                                  's ago) and ' + 
                                  str(self.vmtypes[vmtypeName].notPassedFizzle) + 
                                  ' running but not yet passed fizzle_seconds (' + 
                                  str(self.vmtypes[vmtypeName].fizzle_seconds) + ')')
          continue

        if (not bestVmtypeName) or (self.vmtypes[vmtypeName].weightedMachines < self.vmtypes[bestVmtypeName].weightedMachines):
          bestVmtypeName = vmtypeName
                 
      if bestVmtypeName:
        vcycle.vacutils.logLine('Free capacity found for ' + bestVmtypeName + ' within ' + self.spaceName + ' ... creating')

        # This tracks creation attempts, whether successful or not
        creationsThisCycle += 1

        try:
          self.createMachine(bestVmtypeName)
        except Exception as e:
          vcycle.vacutils.logLine('Failed creating machine with vmtype ' + bestVmtypeName + ' in ' + self.spaceName + ' (' + str(e) + ')')
        else:
          # For newly created machines, we update totals but not self.machines[] objects
          self.totalMachines += 1
          self.vmtypes[bestVmtypeName].totalMachines    += 1
          self.vmtypes[bestVmtypeName].weightedMachines += (1.0 / self.vmtypes[bestVmtypeName].target_share)
          self.vmtypes[bestVmtypeName].notPassedFizzle  += 1

      else:
        vcycle.vacutils.logLine('No more free capacity and/or suitable vmtype found within ' + self.spaceName)
        return
      
  def createMachine(self, vmtypeName):
    # Generic machine creation
  
    machineName = 'vcycle-' + vmtypeName + '-' + str(time.time()).replace('.','-')

    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/machinefeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/jobfeatures',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
    os.makedirs('/var/lib/vcycle/machines/' + machineName + '/machineoutputs',
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/vmtype_name', vmtypeName,  0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/space_name',  self.spaceName,   0644, '/var/lib/vcycle/tmp')

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/phys_cores',    '1',        0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/machinefeatures/shutdown_time', 
                               str(int(time.time()) + self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')

    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/cpu_limit_secs',  
                               str(self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')
    vcycle.vacutils.createFile('/var/lib/vcycle/machines/' + machineName + '/jobfeatures/wall_limit_secs', 
                               str(self.vmtypes[vmtypeName].max_wallclock_seconds), 0644, '/var/lib/vcycle/tmp')

    try:
      userDataContents = vcycle.vacutils.createUserData(shutdownTime   = int(time.time() +
                                                                             self.vmtypes[vmtypeName].max_wallclock_seconds),
                                                        vmtypesPath    = '/var/lib/vcycle/vmtypes/' + self.spaceName,
                                                        options        = self.vmtypes[vmtypeName].options,
                                                        versionString  = 'Vcycle ' + vcycle.shared.vcycleVersion,
                                                        spaceName      = self.spaceName,
                                                        vmtypeName     = vmtypeName,
                                                        userDataPath   = self.vmtypes[vmtypeName].user_data,
                                                        hostName       = machineName,
                                                        uuidStr        = None)
    except Exception as e:
      raise NameError('Failed getting user_data file (' + str(e) + ')')

    try:
      open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'w').write(userDataContents)
    except:
      raise NameError('Failed to writing /var/lib/vcycle/machines/' + machineName + '/user_data')

    return machineName

  def oneCycle(self):
  
    try:
      self.connect()
    except Exception as e:
      vcycle.vacutils.logLine('Skipping ' + self.spaceName + ' this cycle: ' + str(e))
      return

    try:
      self.scanMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Giving up on ' + self.spaceName + ' this cycle: ' + str(e))
      return
      
    try:
      self.cleanupMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Deleting old machines in ' + self.spaceName + ' fails: ' + str(e))
      # We carry on because this isn't fatal
      
    try:
      self.makeMachines()
    except Exception as e:
      vcycle.vacutils.logLine('Making machines in ' + self.spaceName + ' fails: ' + str(e))
      
class OpenstackSpace(BaseSpace):

  def __init__(self, api, spaceName, parser, spaceSectionName):
  # Initialize data structures from configuration files

    # Generic initialization
    BaseSpace.__init__(self, api, spaceName, parser, spaceSectionName)

    # OpenStack-specific initialization
    try:
      self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
    except Exception as e:
      raise NameError('tenancy_name is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.identityURL = parser.get(spaceSectionName, 'url')
    except Exception as e:
      raise NameError('url is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      self.username = parser.get(spaceSectionName, 'username')
    except Exception as e:
      raise NameError('username is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

    try:
      # We use ROT-1 (A -> B etc) encoding so browsing around casually doesn't
      # reveal passwords in a memorable way. 
      self.password = ''.join([ chr(ord(c)-1) for c in parser.get(spaceSectionName, 'password')])
    except Exception as e:
      raise NameError('password is required in OpenStack [space ' + spaceName + '] (' + str(e) + ')')

  def connect(self):
  # Connect to the OpenStack service
  
    try:
      response = self.httpJSON(self.identityURL + '/tokens',
                               { 'auth' : { 'tenantName'           : self.tenancy_name, 
                                            'passwordCredentials' : { 'username' : self.username, 
                                                                      'password' : self.password 
                                                                    }
                                          }
                               } )
    except Exception as e:
      raise NameError('Cannot connect to ' + self.identityURL + ' (' + str(e) + ')')

    self.token     = str(response['access']['token']['id'])
    
    self.computeURL = None
    self.imageURL   = None
    
    for endpoint in response['access']['serviceCatalog']:
      if endpoint['type'] == 'compute':
        self.computeURL = str(endpoint['endpoints'][0]['publicURL'])
      elif endpoint['type'] == 'image':
        self.imageURL = str(endpoint['endpoints'][0]['publicURL'])
        
    if not self.computeURL:
      raise NameError('No compute service URL found from ' + self.identityURL)

    if not self.imageURL:
      raise NameError('No image service URL found from ' + self.identityURL)

  def scanMachines(self):
  # Query OpenStack compute service for details of machines in this space
  
    try:
      response = self.httpJSON(self.computeURL + '/servers/detail',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise NameError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    for oneServer in response['servers']:

      # This includes VMs that we didn't create and won't manage, to avoid going above space limit
      self.totalMachines += 1

      # Just in case other VMs are in this space
      if oneServer['name'][:7] != 'vcycle-':
        continue

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
        startedTime = calendar.timegm(time.strptime(str(oneServer['OS-SRV-USG:launched_at']
                                                       )
                                                   ).split('.')[0], "%Y-%m-%dT%H:%M:%S")
      except:
        startedTime = None

      taskState  = str(oneServer['OS-EXT-STS:task_state'])
      powerState = int(oneServer['OS-EXT-STS:power_state'])
      status     = str(oneServer['status'])
      
      if taskState == 'Deleting':
        state = MachineState.deleting
      elif status == 'ACTIVE' and powerState == 1:
        state = MachineState.running
      elif status == 'BUILD' or status == 'ACTIVE':
        state = MachineState.starting
      elif status == 'SHUTOFF':
        state = MachineState.shutdown
      elif status == 'ERROR':
        state = MachineState.failed
      elif status == 'DELETED':
        state = MachineState.deleting
      else:
        state = MachineState.unknown

      self.machines[oneServer['name']] = vcycle.shared.Machine(name        = oneServer['name'],
                                                               state       = state,
                                                               ip          = ip,
                                                               createdTime = createdTime,
                                                               startedTime = startedTime,
                                                               updatedTime = updatedTime)

  def getFlavorID(self, vmtypeName):
  # Get the "flavor" ID (## "We're all living in Amerika!" ##)
  
    if hasattr(self.vmtypes[vmtypeName], '_flavorID'):
      if self.vmtypes[vmtypeName]._flavorID:
        return self.vmtypes[vmtypeName]._flavorID
      else:
        raise NameError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')
      
    try:
      response = self.httpJSON(self.computeURL + '/flavors',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise NameError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
    
    try:
      for flavor in response['flavors']:
        if flavor['name'] == self.vmtypes[vmtypeName].flavor_name:
          self.vmtypes[vmtypeName]._flavorID = str(flavor['id'])
          return self.vmtypes[vmtypeName]._flavorID
    except:
      pass
        
    raise NameError('Flavor "' + self.vmtypes[vmtypeName].flavor_name + '" for vmtype ' + vmtypeName + ' not available!')

  def getImageID(self, vmtypeName):
  # Get the image ID

    if hasattr(self.vmtypes[vmtypeName], '_imageID'):
      if self.vmtypes[vmtypeName]._imageID:
        return self.vmtypes[vmtypeName]._imageID
      else:
        raise NameError('Image "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' not available!')
      
    try:
      response = self.httpJSON(self.computeURL + '/images/detail',
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise NameError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')
    
    for image in response['images']:
      try:
        if self.vmtypes[vmtypeName].root_image[:6] == 'image:' and \
           self.vmtypes[vmtypeName].root_image[6:] == image['name']:
          self.vmtypes[vmtypeName]._imageID = str(image['id'])
          return self.vmtypes[vmtypeName]._imageID
        # also check for http(s):// images
        # also check for local file images
      except:
        pass
        
    raise NameError('Image "' + self.vmtypes[vmtypeName].root_image + '" for vmtype ' + vmtypeName + ' not available!')
      
  def createMachine(self, vmtypeName):
  
    # Generic machine creation
    try:
      machineName = BaseSpace.createMachine(self, vmtypeName)
    except Exception as e:
      raise NameError('Failed to create new machine: ' + str(e))

    try:
      request = { 'server' : 
                  { 'user_data' : base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                    'name'      : machineName,
                    'imageRef'  : self.getImageID(vmtypeName),
                    'flavorRef' : self.getFlavorID(vmtypeName),
                    'metadata'  : { 'cern-services'   : 'false',
                                    'machinefeatures' : 'https://' + os.uname()[1] + '/' + machineName + '/machinefeatures',
                                    'jobfeatures'     : 'https://' + os.uname()[1] + '/' + machineName + '/jobfeatures',
                                    'machineoutputs'  : 'https://' + os.uname()[1] + '/' + machineName + '/machineoutputs' }
                  }    
                }

      if self.vmtypes[vmtypeName].root_public_key:
        request['key_name'] = self.vmtypes[vmtypeName].root_public_key
        
    except Exception as e:
      raise NameError('Failed to create new machine: ' + str(e))
                                    
    try:
      response = self.httpJSON(self.computeURL + '/servers',
                               request,
                               headers = [ 'X-Auth-Token: ' + self.token ])
    except Exception as e:
      raise NameError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

    vcycle.vacutils.logLine('Created ' + machineName + ' (' + str(response['servers']['id']) + ') for ' + vmtypeName + ' within ' + self.spaceName)

    return machineName

def readConf():

  global vcycleVersion, spaces

  try:
    f = open('/var/lib/vcycle/VERSION', 'r')
    vcycleVersion = f.readline().split('=',1)[1].strip()
    f.close()
  except:
    vcycleVersion = '0.0.0'
  
  spaces = {}

  parser = ConfigParser.RawConfigParser()
  
  # Look for configuration files in /etc/vcycle.d
  try:
    confFiles = os.listdir('/etc/vcycle.d')
  except:
    pass
  else:
    for oneFile in sorted(confFiles):
      if oneFile[-5:] == '.conf':
        try:
          parser.read('/etc/vcycle.d/' + oneFile)
        except Exception as e:
          vcycle.vacutils.logLine('Failed to parse /etc/vcycle.d/' + oneFile + ' (' + str(e) + ')')

  # Standalone configuration file, read last in case of manual overrides
  parser.read('/etc/vcycle.conf')

  # Find the space sections
  for spaceSectionName in parser.sections():
 
    try:
      (sectionType, spaceName) = spaceSectionName.lower().split(None,1)
    except Exception as e:
      raise NameError('Cannot parse section name [' + spaceSectionName + '] (' + str(e) + ')')
    
    if sectionType == 'space':
    
      if string.translate(spaceName, None, '0123456789abcdefghijklmnopqrstuvwxyz-.') != '':
        raise NameError('Name of space section [space ' + spaceName + '] can only contain a-z 0-9 - or .')
      
      try:
        api = parser.get(spaceSectionName, 'api').lower()
      except:
        raise NameError('api missing from [space ' + spaceName + ']')
            
      if api.capitalize()+'Space' not in globals():
        raise NameError(api + ' is not a supported API for managing spaces')

      try:
        # construct an object for this space
        spaces[spaceName] = ( globals()[ api.capitalize()+'Space' ] )(api, spaceName, parser, spaceSectionName)
      except Exception as e:
        raise NameError('Failed to initialise space ' + spaceName + ' (' + str(e) + ')')

    elif sectionType != 'vmtype':
      raise NameError('Section type ' + sectionType + 'not recognised')

  # else: Skip over vmtype sections, which are parsed during the class initialization

def logMachineoutputs(hostName, vmtypeName, spaceName):

  if os.path.exists('/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName):
    # Copy (presumably) already exists so don't need to do anything
    return
   
  try:
    os.makedirs('/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName,
                stat.S_IWUSR + stat.S_IXUSR + stat.S_IRUSR + stat.S_IXGRP + stat.S_IRGRP + stat.S_IXOTH + stat.S_IROTH)
  except:
    vcycle.vacutils.logLine('Failed creating /var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName)
    return
      
  try:
    # Get the list of files that the VM wrote in its /etc/machineoutputs
    outputs = os.listdir('/var/lib/vcycle/machines/' + hostName + '/machineoutputs')
  except:
    vcycle.vacutils.logLine('Failed reading /var/lib/vcycle/machines/' + hostName + '/machineoutputs')
    return
        
  if outputs:
    # Go through the files one by one, adding them to the machineoutputs directory
    for oneOutput in outputs:

      try:
        # first we try a hard link, which is efficient in time and space used
        os.link('/var/lib/vcycle/machines/' + hostName + '/machineoutputs/' + oneOutput,
                '/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName + '/' + oneOutput)
      except:
        try:
          # if linking failed (different filesystems?) then we try a copy
          shutil.copyfile('/var/lib/vcycle/machines/' + hostName + '/machineoutputs/' + oneOutput,
                          '/var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName + '/' + oneOutput)
        except:
          vcycle.vacutils.logLine('Failed copying /var/lib/vcycle/machines/' + hostName + '/machineoutputs/' + oneOutput + 
                  ' to /var/lib/vcycle/machineoutputs/' + spaceName + '/' + vmtypeName + '/' + hostName + '/' + oneOutput)

  