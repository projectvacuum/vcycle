__author__ = 'Luis Villazon Esteban'

from azure.servicemanagement import *
from subprocess import Popen, PIPE 
import base64
import uuid
import logging


FORMAT = "%(asctime)-15s %(clientip)s %(user)-8s %(message)s"
logging.basicConfig(format=FORMAT)

class Azure():
    
    
    
    def __init__(self, publish_settings):
        subscription_id = get_certificate_from_publish_settings(
            publish_settings_path=publish_settings,
            path_to_write_certificate='tmpcert.pem'
        )
        self.sms = ServiceManagementService(subscription_id, 'tmpcert.pem')
        self._generate_pfx('tmpcert.pem')
        
        self.logger = logging.getLogger('azure')



    def list_vms(self, service_name):
       roles = []
       service  = self.sms.get_hosted_service_properties(service_name, True)
       for deployment in service.deployments:
             for role in deployment.role_instance_list:
                roles.append({'name':role.role_name,
                              'status':role.instance_status,
                              'ip':role.instance_endpoints[0].vip})
       return roles
   
   
    def delete_vm(self, service_name, name):
      self.sms.delete_role(service_name, service_name, name);
              
          
    def create_virtual_machine(self, service_name, affinity, vm_name=None, location='West Europe', username=None, password=None,
                               image_name=None, user_data=None, ignore_host=True):
        """Created a new virtual machine
        name Virtual machine name
        username , username to use to connect via SSH to the VM
        password password to connect via SSH to the VM
        image_name Name of the image to use.
        image_name: object
        user_data custom data used to contextualize the VM
        ignore_host: If host exists does not raise an exception when try to create it.
        """
        if service_name is None:
           raise Exception("Service name is mandatory")
        if affinity is None:
           raise Exception("Affinity is mandatory")
        if image_name is None:
            raise Exception("Image name is mandatory")
        
        service_name = service_name.lower()
        affinity = affinity.lower()
        
        self._create_affinity_group(affinity,location)
        
        try:
            self._create_service(service_name, affinity)
        except Exception,e:
           if not ignore_host:
              self.logger.info("Service %s already exists, skipping service creation" % service_name)
              raise e
        
        storage = self._create_storage(service_name, affinity)
        media_link = storage.storage_service_properties.endpoints[0]+"/vhd/"
        
        try:
            self.sms.get_deployment_by_name(service_name,service_name)
            self._create_role(service_name,
                             vm_name=vm_name,
                             image_name=image_name,
                             media_link=media_link,
                             user_data=user_data,
                             username=username,
                             password=password)
        except azure.WindowsAzureMissingResourceError,e:
            self._create_deployment(service_name,
                                    affinity,
                                    vm_name=vm_name,
                                    username=username,
                                    password=password,
                                    user_data=user_data,
                                    image_name=image_name,
                                    media_link=media_link)

    
           
    def _create_deployment(self, service_name, affinity_group, vm_name=None, image_name=None,
                   media_link=None, user_data=None, username=None, password=None):
        """
        "Creates VM
        ' service_name  name Name
        ' affinity_group Name of the affinity group where the VM will be created
        ' username username to use to connect via SSH to the VM
        ' password password to use to connect via SSH to the VM
        ' location location where the VM will be allocated.
        ' image_name Name of the image to use
        ' media_link link to store the VM
        """
        if vm_name is None:
           vm_name = str(uuid.uuid4())
        values = self._common_for_deployments_and_roles(vm_name, username, password, user_data, image_name, media_link)
        response = self.sms.create_virtual_machine_deployment(service_name, service_name, 'production', service_name, vm_name, values['linux_config'],
                                                   values['os_hd'],
                                                   availability_set_name=affinity_group,
                                                   network_config=values['configuration_set'],
                                                   role_size='Small')
        self.sms.wait_for_operation_status(response.request_id)
        self.logger.info("Created new deployment %s" % service_name)
        self.logger.info("Created new role %s" % vm_name)


    def _create_role(self, service_name, vm_name=None, image_name=None,
                     media_link=None, user_data=None, username=None, password=None):
        
        def failure_callback(elapsed, ex):
           print ex
           print elapsed
        if vm_name is None:
            vm_name = str(uuid.uuid4())
        values = self._common_for_deployments_and_roles(vm_name, username, password, user_data, image_name, media_link)
        response = self.sms.add_role(service_name, service_name,
                                     vm_name, values['linux_config'],
                                     values['os_hd'],network_config=values['configuration_set'],
                                    role_size='Small')
        self.sms.wait_for_operation_status(response.request_id,failure_callback=failure_callback)
        self.logger.info("Created new role %s" % vm_name)
   
   
    def _common_for_deployments_and_roles(self, name, username, password, user_data, image_name, media_link):
        fingerprint = self._generate_certificate_fingerprint("tmpcert.pem")
        linux_config = azure.servicemanagement.LinuxConfigurationSet(name, username, password, True)
        linux_config.ssh.public_keys.public_keys.append(PublicKey(fingerprint,"/home/%s/.ssh/authorized_keys" % username))
        if user_data:
           linux_config.custom_data = open(user_data,'r').read()
           
        os_hd = azure.servicemanagement.OSVirtualHardDisk(image_name, media_link + name+ ".vhd")
        configuration_set = ConfigurationSet()
        
        #configuration_set.input_endpoints.input_endpoints.append(
        #        ConfigurationSetInputEndpoint(name=u'SSH', protocol=u'TCP', port=u'22', local_port=u'22'))
        
        return {'linux_config':linux_config,
                'configuration_set':configuration_set,
                'os_hd':os_hd,
                'user_data':user_data}
        
    
    def _generate_certificate_fingerprint(self,certificate):
        command = "openssl x509 -in %s -noout -fingerprint" % (certificate )
        (result,err_result)=Popen(command,bufsize=1,shell=True,stdout=PIPE,stderr=PIPE).communicate()
        return result[result.find('=')+1:].replace(':','').strip()
    
    
    def _generate_pfx(self, certificate):
        command = "echo "" | openssl pkcs12 -passout stdin -inkey %s -in %s -export -out tempcert.pfx" % (certificate, certificate)
        (result,err_result)=Popen(command,bufsize=1,shell=True,stdout=PIPE,stderr=PIPE).communicate()
    
    
    #==================== Storage Operations ========================================#
  
    def _list_storages(self):
       return self.sms.list_storage_accounts()
   
    
    def _exists_storage(self, service_name):
       for storage in self.sms.list_storage_accounts():
          if storage.service_name == service_name:
             return True
       return False
    
    
    def _create_storage(self, name, affinity):
        """
        ' Creates a new storage
        ' name Storage name.
        ' location location where the storage will be created.
        """
        if not self._exists_storage(name):
            result = self.sms.create_storage_account(name, name, name, affinity)
            self.sms.wait_for_operation_status(result.request_id)
            self.logger.info("Storage %s created!" % name)
        else:
            self.logger.info("Storage %s already exists, skipping storage creation" % name)
        return self.sms.get_storage_account_properties(name)
        
        

    def _delete_storage(self, name):
        """
        ' Deletes storage
        ' name Name of the storage to delete
        """
        result = self.sms.delete_storage_account(name)


    def list_deployments(self):
       deployments = []
       for host in self.get_hosts():
          for deployment in self.get_deployments_from_host(host['name']):
              deployments.append(deployment)
       return deployments
          
          
    def list_services(self):
        """
        ' Return a list of hosts
        """
        services = []
        for service in self.sms.list_hosted_services():
            services.append({'url':service.url, 'name':service.service_name})
        return services

              
    def _create_service(self, name, affinity_group):
        """
        Creates a new host
        name Name of the host
        affinity_group affinity_group where host will be created
        If create_affinity_group is True and the affinity_group does not exists, it will be created
        Location is only necesary if create_affinity_group is True
        """
        if not self.sms.check_hosted_service_name_availability(name).result:
            raise Exception("Name host is not available")
         
        request = self.sms.create_hosted_service(name, name, name, affinity_group=affinity_group)
        if request != None:
           self.sms.wait_for_operation_status(request.request_id)
        self.logger.info("Service %s created!" % name)
        f = open("tempcert.pfx",'r')
        text = f.read()
        data = base64.b64encode(text)
        request = self.sms.add_service_certificate(name, data, 'pfx', "")
        if request != None:
           try:
               self.sms.wait_for_operation_status(request.request_id)
           except Exception,e:
              raise("Check error " + e)
        self.logger.info("Certificate added to service %s" % name)
        return True
     
     
    def _delete_service(self, name):
       request = self.sms.delete_hosted_service(name)
   
    #====================== Affinity group operations ==================================================#
       
    def _list_affinity_group(self):
       return self.sms.list_affinity_groups()
             
   
    def _exist_affinity_group(self, name):
       """
       Checks if an affinity group exists
       """
       affinity_groups = self._list_affinity_group()
       if len(affinity_groups) == 0:
          return False
       
       for affinity_group in affinity_groups:
          if name == affinity_group.name.lower():
             return True
       return False
    
    
    def _create_affinity_group(self, name, location="West Europe"):
        """
        Creates new affinity_group
        """
        if self._exist_affinity_group(name):
           self.logger.info("Affinity %s already exists, skipping creation" % name)
           return True
        
        request = self.sms.create_affinity_group(name, name, location)
        if request != None:
           try:
              self.sms.wait_for_operation_status(request.request_id)
              self.logger.info("Affinity %s created!" % name)
           except:
              return True
        return True
     
    
    def _delete_affinity_group(self, name):
       try:
           self.sms.delete_affinity_group(name)
       except Exception,e:
           print e
       finally:
          return True


    def delete_deployment(self, name):
       request = self.sms.delete_deployment(name, name, True)
       if request != None:
          self.sms.wait_for_operation_status(request.request_id)
       request = self.sms.delete_hosted_service(name)
       if request != None:
          self.sms.wait_for_operation_status(request.request_id)
       return True
    
   
       
    
class Server(object):
   def __init__(self, client):
      self.client = client
      
   @staticmethod   
   def new(self, client, service_name, name, status, created_time, updated_time, roles=None):
      server = Server(client)
      server.id = id
      server.service_name = service_name
      server.name = name
      server.status = status
      server.created = created_time
      server.updated = updated_time
      server.roles = roles
      server.ip = None
      
      if len(server.roles) > 0 and server.roles[0]['status'] == 'StoppedVM':
         server.status = server.roles[0]['status']
      if len(server.roles) > 0 and 'ip' in server.roles[0]:
         server.ip = server.roles[0]['ip']
         
      if server.updated is None:
         server.updated = server.created
      return server
   
   
   def delete(self):
      return self.client.delete_deployment(self.service_name,self.name, True)
   
   
   def __repr__(self):
      return "%s %s %s" % (self.name, self.status, self.created)

