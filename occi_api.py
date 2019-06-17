#!/usr/bin/python
#
#  occi_api.py - common functions, classes, and variables for Vcycle
#
#  THIS FILE NEEDS UPDATING FOR Vcycle 3.0 CHANGES!
#
#  Andrew McNab, University of Manchester.
#  Luis Villazon Esteban, CERN.
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
#            Luis.Villazon.Esteban@cern.ch
#


import requests
import time
import base64
import vcycle.vacutils



class OcciError(Exception):
  pass


ca_path = '/etc/grid-security/occi.ca-certs'

class OcciSpace(vcycle.BaseSpace):

    def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
        # Initialize data structures from configuration files

        # Generic initialization
        vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

        # OCCI-specific initialization
        try:
            self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
        except Exception as e:
            raise OcciError('tenancy_name is required in Occi [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.queryURL = parser.get(spaceSectionName, 'url')
        except Exception as e:
            raise OcciError('url is required in Occi [space ' + spaceName + '] (' + str(e) + ')')

        if self.queryURL.endswith('/'):
            self.queryURL = self.queryURL[:-1]

        #check if proxy is in the configuration, if not, then use the username-password
        if parser.has_option(spaceSectionName, 'proxy'):
            self.userkey = parser.get(spaceSectionName, 'proxy')
            self.usercert = parser.get(spaceSectionName, 'proxy')

        else:
            #check username and password are defined
            if not parser.has_option(spaceSectionName, 'username'):
                raise OcciError('username is required in Occi [space  %s]' % spaceName)
            if not parser.has_option(spaceSectionName, 'password'):
                raise OcciError('password is required in Occi [space  %s]' % spaceName)
            self.username = parser.get(spaceSectionName, 'username')
            self.password = ''.join([ chr(ord(c)-1) for c in parser.get(spaceSectionName, 'password')])

        self._create_ca_file()

    def connect(self):
        # Connect to the OCCI service
        self.session = requests.Session()
        self.session.mount(self.queryURL, requests.adapters.HTTPAdapter(pool_connections=20))

        #Retrieve token
        keystone_url = self._get_keystone()
        if keystone_url is not None:
            vcycle.vacutils.logLine("Found Keystone URL %s" % keystone_url)
            self.token = self._get_token(keystone_url)
            self.session.headers.clear()
            self.session.headers.update({"X-Auth-Token": self.token})
            self.session.cert = self.usercert
            self.session.verify = ca_path

        self._get_definitions()
        self.computeURL = "%s/compute/" % (self.queryURL)
        vcycle.vacutils.logLine("Connected to %s for space %s" % (self.queryURL ,self.spaceName))

    def scanMachines(self):
        """Query OCCI compute service for details of machines in this space"""
        headers = {'Accept': 'application/occi+json',
                   'Content-Type': 'application/occi+json'}
        try:
            response = self.session.get(self.computeURL)
        except Exception as e:
            raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

        # Convert machines from None to an empty dictionary since we successfully connected
        self.machines = {}

        for machineID in [line[line.rfind('/')+1:] for line in response.text.split("\n")[1:]]:
            try:
                response = self.session.get("%s/%s" % (self.computeURL, machineID), headers=headers)
            except Exception as e:
                raise OcciError('Cannot connect to %s/%s (%s)' %(self.computeURL, machineID, str(e)))

            response = response.json()
            machineName = response['attributes']['occi.compute.hostname']
            occiState   = response['attributes']['org.openstack.compute.state'].lower()
            uuidStr     = response['attributes']['occi.core.id']
            try:
                ip = response['links'][0]['attributes']['occi.networkinterface.address']
            except:
                ip = '0.0.0.0'
            # Just in case other VMs are in this space
            if machineName[:7] != 'vcycle-':
                # Still count VMs that we didn't create and won't manage, to avoid going above space limit
                self.totalProcessors += 1 # FIXME: GET THE REAL NUMBER NOT JUST 1
                continue

            # With OCCI will have to use our file datestamps to get transition times
            try:
                createdTime = int(open('/var/lib/vcycle/machines/' + machineName + '/started', 'r').read().strip())
                updatedTime = int(open('/var/lib/vcycle/machines/' + machineName + '/started', 'r').read().strip())
                startedTime = int(open('/var/lib/vcycle/machines/' + machineName + '/started', 'r').read().strip())
            except:
                createdTime = None
                updatedTime = None
                startedTime = None

            if occiState == 'active':
                state = vcycle.MachineState.running
            elif occiState == 'inactive':
                state = vcycle.MachineState.shutdown
            else:
                state = vcycle.MachineState.unknown

            self.machines[machineName] = vcycle.shared.Machine(name=machineName,
                                                 spaceName=self.spaceName,
                                                 state=state,
                                                 ip=ip,
                                                 createdTime=createdTime,
                                                 startedTime=startedTime,
                                                 updatedTime=updatedTime,
                                                 uuidStr=uuidStr,
                                                 machinetypeName=None)

    def createMachine(self, machineName, machinetypeName, zone = None):

        # OCCI-specific machine creation steps
        #    'metadata'  : { 'cern-services'   : 'false',
        #                    'machinefeatures' : 'http://'  + os.uname()[1] + '/' + machineName + '/machinefeatures',
        #                    'jobfeatures'     : 'http://'  + os.uname()[1] + '/' + machineName + '/jobfeatures',
        #                    'machineoutputs'  : 'https://' + os.uname()[1] + '/' + machineName + '/machineoutputs' }

        import uuid
        headers = {'X-Auth-Token': self.token,
                   'Accept': 'text/plain,text/occi',
                   'Content-Type': 'text/plain,text/occi',
                   'Connection': 'close'
        }

        image = self.machinetypes[machinetypeName].root_image[6:].strip()
        data = 'Category: compute;scheme="http://schemas.ogf.org/occi/infrastructure#";class="kind";location="/compute/";title="Compute Resource"\n'
        data += 'Category: %s;%s;class="mixin";location="/%s"\n' % (image, self.categories[image]['scheme'], image)
        data += 'Category: %s;%s;class="mixin";location="/%s"\n' % (self.machinetypes[machinetypeName].flavor_names[0], self.categories[self.machinetypes[machinetypeName].flavor_names[0]]['scheme'], self.machinetypes[machinetypeName].flavor_names[0])
        data += 'Category: user_data;"%s";class="mixin";location="%s";title="OS contextualization mixin"\n' % (self.categories['user_data']['scheme'], self.categories['user_data']['location']);
        data += 'X-OCCI-Attribute: occi.core.id="%s"\n' % str(uuid.uuid4())
        data += 'X-OCCI-Attribute: occi.core.title="%s"\n' % machineName
        data += 'X-OCCI-Attribute: occi.compute.hostname="%s"\n' % machineName
        data += 'X-OCCI-Attribute: org.openstack.compute.user_data="%s"' % base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read())

        if self.machinetypes[machinetypeName].root_public_key:
            if self.machinetypes[machinetypeName].root_public_key[0] == '/':
                try:
                    f = open(self.machinetypes[machinetypeName].root_public_key, 'r')
                except Exception as e:
                    OcciError('Cannot open ' + self.machinetypes[machinetypeName].root_public_key)
            else:
                try:
                    f = open('/var/lib/vcycle/' + self.spaceName + '/' + self.machinetypeName + '/' + self.machinetypes[machinetypeName].root_public_key, 'r')
                except Exception as e:
                    OcciError('Cannot open ' + self.spaceName + '/' + self.machinetypeName + '/' + self.machinetypes[machinetypeName].root_public_key)

            while True:
                try:
                    line = f.read()
                except:
                    raise OcciError('Cannot find ssh-rsa public key line in ' + self.machinetypes[machinetypeName].root_public_key)

                if line[:8] == 'ssh-rsa ':
                    sshPublicKey = line.split(' ')[1]
                    data += 'X-OCCI-Attribute: org.openstack.credentials.publickey.data="ssh-rsa ' + sshPublicKey + ' vcycle"'
                    break

        try:
            response = self.session.post(self.computeURL, data=data, headers=headers)
            if response.status_code not in [200, 201]:
                raise OcciError(response.text)
        except Exception as e:
            raise OcciError('Cannot connect to ' + self.computeURL + ' (' + str(e) + ')')

        vcycle.vacutils.logLine('Created ' + machineName + ' for ' + machinetypeName + ' within ' + self.spaceName)

        self.machines[machineName] = vcycle.shared.Machine(name=machineName,
                                             spaceName=self.spaceName,
                                             state=vcycle.MachineState.starting,
                                             ip='0.0.0.0',
                                             createdTime=int(time.time()),
                                             startedTime=int(time.time()),
                                             updatedTime=int(time.time()),
                                             uuidStr=None,
                                             machinetypeName=machinetypeName)

        return machineName

    def deleteOneMachine(self, machineName):
        """Deletes a VM from the provider

        :param machineName: vm identifier
        """
        try:
            self.session.delete("%s%s" % (self.computeURL, self.machines[machineName].uuidStr))
        except Exception as e:
            raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' via ' + self.computeURL + ' (' + str(e) + ')')

    def _get_definitions(self):
        """Store the schema definitions to create VMs

        """
        headers = {'X-Auth-Token': self.token,
                   'Accept': 'text/plain,text/occi'}

        response = requests.get("%s/-/" % self.queryURL,
                                    headers=headers,
                                    cert=self.usercert,
                                    verify=ca_path)

        self.categories = {}
        categories = response.text.split("\n")[1:]
        for category in categories:
            values = category.split(";")
            cat = values[0][values[0].find(":")+1:].strip()
            self.categories[cat] = {}
            for property in values:
                if property.find("scheme=") >= 0:
                    self.categories[cat]["scheme"] = property.strip()
                if property.find("class=") >= 0:
                    self.categories[cat]["class"] = property.strip()
                if property.find("title=") >= 0:
                    self.categories[cat]["title"] = property.strip()
                if property.find("location=") >= 0:
                    aux = property.strip()
                    aux = aux.replace("https://","")
                    aux = aux.replace("http://","")
                    aux = aux[aux.find("/"):]
                    self.categories[cat]["location"] = 'location="'+aux

    def _get_keystone(self):
        """ Returns The authorization token to retrieve the OCCI token

        :return: The keystone url
        """
        try:
            result = requests.head(self.queryURL + '/-/',
                                   headers={"Content-Type": "application/json"},
                                   cert=self.usercert,
                                   verify=ca_path
                                   )
        except Exception as e:
            raise OcciError('Cannot connect to ' + self.queryURL + ' (' + str(e) + ')')

        # This is implicitly only for Keystone authentication
        if result.status_code != 401 or result.headers is None:
            raise OcciError('Do not recognise response when connecting to ' + self.queryURL)

        if 'www-authenticate' not in result.headers:
            return None

        # Explicitly check for Keystone using hard-coded string index values for now
        if not result.headers['www-authenticate'].startswith("Keystone uri="):
            raise OcciError('Only Keystone authentication is currently supported (instead got "%s")' %
                            result.headers['www-authenticate'])

        try:
            keystoneURL = result.headers['www-authenticate'][14:-1]
            keystoneURL = keystoneURL.replace("/v2.0", '')
        except:
            raise OcciError("Failed to find Keystone URL in %s" % result.headers['www-authenticate'])
        return keystoneURL

    def _get_token(self, keystone_url):
        """ Returns The token to request OCCI site

        :param keystone_url: URL to do the request
        :return: The token
        """
        if self.userkey is not None:
            auth = {'auth': {'voms': True}}
        else:
            auth = {'auth': {
                        'passwordCredentials': {
                            'username': self.username,
                            'password': self.password
                            }
                        }
                    }
        try:
            result = {'response':requests.post(keystone_url+"/v2.0/tokens",
                                 data='{"auth":{"voms": true}}',
                                 headers={"Content-Type": "application/json"},
                                 cert=self.usercert, verify=ca_path).json()}
        except Exception as e:
            raise OcciError('Cannot connect to ' + keystone_url + ' (' + str(e) + ')')

        token = str(result['response']['access']['token']['id'])
        tenants = self._get_tenants(keystone_url, token)
        return self.__auth_in_tenant(keystone_url, token, tenants)

    def _get_tenants(self, keystone_url, temporal_token):
        """ Returns all the tenants available in the provider

        :param token: Authorization token
        :return: The name of all tenants
        """
        result = {'response': requests.get("%s/v2.0/tenants/" % keystone_url,
                                           data='{"auth":{"voms": true}}',
                                           headers={"Content-Type": "application/json", "X-Auth-Token": temporal_token},
                                           cert=self.usercert,
                                           verify=ca_path).json()}

        return [tenant['name'] for tenant in result['response']['tenants']]

    def __auth_in_tenant(self, keystone_url, token, tenants):
        """ Returns the token linked to the tenant

        Loop all tenants, trying to authorize the user with  each tenant, ones a tenant is valid, a token is returned

        :param token: System token
        :param tenants:  list of tenants
        :return: token and expiration date
        """
        import json
        for tenant in tenants:
            data = {'auth': {'voms': True, 'tenantName': tenant}}
            headers = {
                'Accept': 'application/json',
                'X-Auth-Token': token,
                'User-Agent': 'Vcycle ' + vcycle.shared.vcycleVersion + ' ( OCCI/1.1 )',
                'Content-Type': 'application/json',
                'Content-Length': len(json.dumps(data))
            }

            try:
                result = {'response': requests.post("%s/v2.0/tokens" % keystone_url,
                                                data=json.dumps(data),
                                                headers= headers,
                                                cert=self.usercert,
                                                verify=ca_path).json()}
            except Exception as e:
                print e
            if 'access' in result['response']:
                return result['response']['access']['token']['id']

    def _create_ca_file(self):
        import subprocess
        import os.path
        if not os.path.exists(ca_path):
            subprocess.call('cat `ls /etc/grid-security/certificates/*.pem` > %s' % ca_path,
                            shell=True)
        else:
            modification_time = os.lstat(ca_path).st_mtime
            for file in os.listdir('/etc/grid-security/certificates/'):
                if os.lstat('/etc/grid-security/certificates/%s' % file).st_mtime > modification_time:
                    subprocess.call('cat `ls /etc/grid-security/certificates/*.pem` > %s' % ca_path,
                            shell=True)
                    return
