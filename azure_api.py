#!/usr/bin/python
#
#  azure_api.py - an Azure plugin for Vcycle
#
#  THIS FILE NEEDS UPDATING FOR Vcycle 3.0 CHANGES!
#
#  Andrew McNab, University of Manchester.
#  Luis Villazon Esteban, CERN.
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
#            Luis.Villazon.Esteban@cern.ch
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

from azure import *
from azure.servicemanagement import *

import vcycle.vacutils

class AzureError(Exception):
    pass


class AzureSpace(vcycle.BaseSpace):

    def __init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
        # Initialize data structures from configuration files
        # Generic initialization
        vcycle.BaseSpace.__init__(self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

        # Azure-specific initialization
        try:
            self.tenancy_name = parser.get(spaceSectionName, 'tenancy_name')
        except Exception as e:
            raise AzureError('tenancy_name is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.subscription = parser.get(spaceSectionName, 'subscription')
        except Exception as e:
            raise AzureError('subscription is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.certificate = parser.get(spaceSectionName, 'certificate')
        except Exception as e:
            raise AzureError('certificate is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.location = parser.get(spaceSectionName, 'location')
        except Exception as e:
            raise AzureError('location is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.pfx = parser.get(spaceSectionName, 'pfx')
        except Exception as e:
            raise AzureError('pfx is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.username = parser.get(spaceSectionName, 'username')
        except Exception as e:
            raise AzureError('username is required in Azure [space ' + spaceName + '] (' + str(e) + ')')

        try:
            self.password = parser.get(spaceSectionName, 'password')
        except Exception as e:
            raise AzureError('password is required in Azure [space ' + spaceName + '] (' + str(e) + ')')


    def connect(self):
        # Connect to the Azure service
        #Nothing to do
        pass

    def scanMachines(self):
        """Query Azure compute service for details of machines in this space"""

        # For each machine found in the space, this method is responsible for
        # either (a) ignorning non-Vcycle VMs but updating self.totalProcessors
        # or (b) creating a Machine object for the VM in self.spaces

        try:
            sms = ServiceManagementService(self.subscription, self.certificate)
            results = sms.list_hosted_services()
        except Exception as ex:
            if 'file' in str(ex):
                raise AzureError("No cert file , check the path.")
            raise AzureError(str(ex))

        # Convert machines from None to an empty dictionary since we successfully connected
        self.machines = {}

        for result in results:
            try:
                info = sms.get_hosted_service_properties(result.service_name, True)
            except WindowsAzureMissingResourceError as ex:
                vcycle.vacutils.logLine("% don't have vms? " % result.service_name)
                continue

            if len(info.deployments) == 0 : continue
            if not result.service_name.startswith('vcycle-'):
                # Still count VMs that we didn't create and won't manage, to avoid going above space limit
                self.totalProcessors += 1 # FIXME: GET THE REAL NUMBER, NOT JUST 1
                continue

            uuidStr = str(result.service_name)
            ip = '0.0.0.0'
            createdTime  = calendar.timegm(time.strptime(result.hosted_service_properties.date_created, "%Y-%m-%dT%H:%M:%SZ"))
            updatedTime  = calendar.timegm(time.strptime(result.hosted_service_properties.date_last_modified, "%Y-%m-%dT%H:%M:%SZ"))
            startedTime = calendar.timegm(time.strptime(result.hosted_service_properties.date_created, "%Y-%m-%dT%H:%M:%SZ"))
            machinetypeName = None

            try:
                status = info.deployments[0].role_instance_list[0].instance_status
                if status in ['Unknown', 'CreatingVM', 'StartingVM', 'CreatingRole', 'StartingRole',
                                         'ReadyRole', 'BusyRole', 'Preparing','ProvisioningFailed']:
                    state = vcycle.MachineState.starting
                elif status in ['StoppingRole', 'StoppingVM', 'DeletingVM',
                                'StoppedVM', 'RestartingRole','StoppedDeallocated']:
                    state = vcycle.MachineState.deleting
                else:
                    state = vcycle.MachineState.starting
            except Exception as ex:
                import json
                vcycle.vacutils.logLine(json.dumps(info,indent=2))
                vcycle.vacutils.logLine(str(ex))
                state = vcycle.MachineState.starting

            self.machines[result.service_name] = vcycle.Machine(name        = result.service_name,
                                                                spaceName   = self.spaceName,
                                                                state       = state,
                                                                ip          = ip,
                                                                createdTime = createdTime,
                                                                startedTime = startedTime,
                                                                updatedTime = updatedTime,
                                                                uuidStr     = uuidStr,
                                                                machinetypeName  = machinetypeName)


    def createMachine(self, machineName, machinetypeName, zone = None):
        try:
            self.__create_service(name=machineName, location=self.location)
            fingerprint, path = self.__add_certificate_to_service(name=machineName, pfx=self.pfx)
            self.__create_vm(name=machineName,
                             flavor=self.machinetypes[machinetypeName].flavor_names[0],
                             image=self.machinetypes[machinetypeName].root_image,
                             username= self.username,
                             password= self.password,
                             user_data=base64.b64encode(open('/var/lib/vcycle/machines/' + machineName + '/user_data', 'r').read()),
                             fingerprint=(fingerprint, path))
            vcycle.vacutils.logLine('Created ' + machineName + ' (' + machineName + ') for ' + machinetypeName + ' within ' + self.spaceName)

            self.machines[machineName] = vcycle.shared.Machine(name        = machineName,
                                                               spaceName   = self.spaceName,
                                                               state       = vcycle.MachineState.starting,
                                                               ip          = '0.0.0.0',
                                                               createdTime = int(time.time()),
                                                               startedTime = None,
                                                               updatedTime = int(time.time()),
                                                               uuidStr     = None,
                                                               machinetypeName  = machinetypeName)
        except Exception as ex:
            try:
                self.__delete(machineName)
                raise AzureError(str(ex))
            except Exception as ex:
                raise AzureError(str(ex))

    def deleteOneMachine(self, machineName):
        sms = ServiceManagementService(self.subscription, self.certificate)
        try:
            sms.delete_hosted_service(machineName, True)
        except Exception as e:
            raise vcycle.shared.VcycleError('Cannot delete ' + machineName + ' (' + str(e) + ')')


    def __create_service(self, name="", location=None):
        """ Create a new service

        :param name: Name of the service
        :param location: Location of the service

        """
        sms = ServiceManagementService(self.subscription, self.certificate)
        result = sms.check_hosted_service_name_availability(name)
        if not result:
            raise AzureError("The service name %s is not available" % name)
        try:
            result = sms.create_hosted_service(name, name, name, location)
            sms.wait_for_operation_status(result.request_id)
        except Exception as ex:
            raise AzureError("The service name %s is not available" % name)

    def __add_certificate_to_service(self, name="", pfx=""):
        """ Adds a certificate into the service.

        The certificate is used to connect via ssh to the VM

        :param name: Name of the service where the certificate will be added
        :param pfx: location on local disk of the certificate to upload

        """
        import base64
        sms = ServiceManagementService(self.subscription, self.certificate)
        result = sms.add_service_certificate(name, base64.b64encode(open(pfx).read()), 'pfx', '')
        sms.wait_for_operation_status(result.request_id)
        list = sms.list_service_certificates(name)
        for certificate in list:
            return certificate.thumbprint, certificate.certificate_url

    def __create_vm(self, name="", flavor="", image="", username="", password="", user_data=None, fingerprint=None):
        """ Creates  new VM

        :param name: Name of the new VM
        :param flavor: Flavor to create the VM
        :param image: Image to create the VM
        :param username: username to use to connect to the vm via SSH
        :param password:  password to use to connect to the vm via SSH
        :param user_data: contextualization file

        """
        sms = ServiceManagementService(self.subscription, self.certificate)

        configuration_set = LinuxConfigurationSet(host_name=name,
                                                  user_name=username,
                                                  user_password=password,
                                                  disable_ssh_password_authentication=False,
                                                  custom_data=user_data)

        if fingerprint is not None:
            configuration_set.ssh.public_keys.public_keys.append(PublicKey(fingerprint=fingerprint[0], path=fingerprint[1]))

        network_set = ConfigurationSet()
        network_set.input_endpoints.input_endpoints.append(ConfigurationSetInputEndpoint(name='SSH',
                                                                                         protocol="TCP",
                                                                                         port=22,
                                                                                         local_port=22))

        result = sms.create_virtual_machine_deployment(name,
                                                       name,
                                                       'production',
                                                       name,
                                                       name,
                                                       configuration_set,
                                                       None,
                                                       network_config= network_set,
                                                       role_size=flavor,
                                                       vm_image_name=image,
                                                       provision_guest_agent=True)

    def __delete(self, identifier):
        """Deletes a VM in the provider

        :param identifier: vm identifier
        """
        sms = ServiceManagementService(self.subscription, self.certificate)
        try:
            sms.delete_hosted_service(identifier, True)
        except Exception as e:
            raise AzureError(str(e))
