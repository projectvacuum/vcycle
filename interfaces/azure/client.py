__author__ = 'Luis Villazon Esteban'

from azure.servicemanagement import *
from subprocess import Popen, PIPE 
import base64
import uuid
import logging


FORMAT = "%(asctime)-15s  %(message)s"
logging.basicConfig(format=FORMAT)

class Azure():
 
    
    def __init__(self, publish_settings, service_name, location="West Europe", ignoreError=True):
        subscription_id = get_certificate_from_publish_settings(
            publish_settings_path=publish_settings,
            path_to_write_certificate='tmpcert.pem'
        )
        self.sms = ServiceManagementService(subscription_id, 'tmpcert.pem')
        self._generate_pfx('tmpcert.pem')
        self.service_name = service_name
        self.logger = logging.getLogger('azure')
        self.logger.setLevel("INFO")
        self.start(location, ignoreError)


    def start(self, location, ignore_error=True):
        '''
        Creates the affinity_group, host service and the storage
        '''
        self._create_affinity_group(location)
        try:
            self._create_service()
        except Exception,e:
           if not ignore_error:
              self.logger.info("Service %s already exists, skipping service creation" % self.service_name)
              raise e
        storage = self._create_storage()


    def list_vms(self):
        """
        Returns a list with all VMs in a host service
        """
        roles = []
        try:
            service  = self.sms.get_hosted_service_properties(self.service_name, True)
        except Exception,e:
           if "Not found" in e.message:
               raise BaseException("Service name not exists")
           else:
               raise e
        for deployment in service.deployments:
              for role in deployment.role_instance_list:
                 roles.append(Server(role))
        return roles
   
   
    def delete_vm(self, name):
        """
        Delete a VM and the associate disk
        """
        response = None
        try:
            response = self.sms.delete_role(self.service_name, self.service_name, name)
        except Exception:
            response = self.sms.delete_deployment(self.service_name, self.service_name, True)
        finally:
           try:
              self.sms.wait_for_operation_status(response.request_id)
           except Exception:
              pass
        
        for disk in self.sms.list_disks():
            if ("%s-%s" %(self.service_name,name)) in disk.name:
                try:
                    response = self.sms.delete_disk(disk.name, delete_vhd=True)
                    self.sms.wait_for_operation_status(response.request_id)
                except Exception,e:
                    print e         
          
          
    def create_virtual_machine(self, vm_name=None, username=None, password=None,
                               image_name=None,flavor="Small", user_data=None):
        """Created a new virtual machine
        vm_name Virtual machine name
        username , username to use to connect via SSH to the VM
        password password to connect via SSH to the VM
        image_name Name of the image to use.
        image_name: object
        user_data custom data used to contextualize the VM
        ignore_host: If host exists does not raise an exception when try to create it.
        """
        if image_name is None:
            raise Exception("Image name is mandatory")
        
        storage = self.sms.get_storage_account_properties(self.service_name)
        if storage.storage_service_properties.status.lower() != 'created':
           self.logger.warn("Storage is creating, skip VM creation")
           return False
        
        media_link = storage.storage_service_properties.endpoints[0]+"/vhd/"
        
        result = None
        try:
            self.sms.get_deployment_by_name(self.service_name,self.service_name)
            result = self._create_role(vm_name=vm_name,
                              image_name=image_name,
                              media_link=media_link,
                              user_data=user_data,
                              flavor=flavor,
                              username=username,
                              password=password)
        except azure.WindowsAzureMissingResourceError,e:
            result = self._create_deployment(vm_name=vm_name,
                                    username=username,
                                    password=password,
                                    user_data=user_data,
                                    flavor=flavor,
                                    image_name=image_name,
                                    media_link=media_link)
        finally:
            role = self.sms.get_role(self.service_name,self.service_name,vm_name)
            return Server(role)
           
    def _create_deployment(self, vm_name=None, image_name=None, flavor=None,
                   media_link=None, user_data=None, username=None, password=None):
        """
        "Creates VM (implies create Development and new role)
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
        values = self._common_for_deployments_and_roles(vm_name,
                                                        username,
                                                        password,
                                                        user_data,
                                                        image_name,
                                                        media_link)
        configuration_set = ConfigurationSet()
        
        configuration_set.input_endpoints.input_endpoints.append(
                ConfigurationSetInputEndpoint(name=u'SSH',
                                              protocol=u'TCP',
                                              port=u'22',
                                              local_port=u'22'))
        try:
            response = self.sms.create_virtual_machine_deployment(self.service_name, 
                                                                  self.service_name,
                                                                  'production',
                                                                  self.service_name,
                                                                  vm_name,
                                                                  values['linux_config'],
                                                                  values['os_hd'],
                                                                  availability_set_name=self.service_name,
                                                                  network_config=configuration_set,
                                                                  role_size=flavor)
            self.sms.wait_for_operation_status(response.request_id)
            self.logger.info("Created new deployment %s" % self.service_name)
            self.logger.info("Created new role %s" % vm_name)
            return True
        except Exception,e:
            self.logger.error("Error creating VM %s" % e.message)
            return False


    def _create_role(self, vm_name=None, image_name=None, flavor=None,
                     media_link=None, user_data=None, username=None, password=None):
        """
        Creates a new role (new VM)
        """
        def failure_callback(elapsed, ex):
           self.logger.error(ex.message)
           raise(ex)
                 
        if vm_name is None:
            vm_name = str(uuid.uuid4())
        values = self._common_for_deployments_and_roles(vm_name,
                                                        username,
                                                        password,
                                                        user_data,
                                                        image_name,
                                                        media_link)
        try:
            response = self.sms.add_role(self.service_name,
                                         self.service_name,
                                         vm_name,
                                         values['linux_config'],
                                         values['os_hd'],
                                         network_config=values['configuration_set'],
                                         role_size=flavor)
            
            self.sms.wait_for_operation_status(response.request_id,failure_callback=failure_callback)
            self.logger.info("Created new role %s" % vm_name)
            return True
        except Exception,e:
            self.logger.error("Error creating VM %s" % e.message)
            return False 
   
   
    def _common_for_deployments_and_roles(self, name, username, password, user_data, image_name, media_link):
        """
        Common information to create deployments and roles
        """
        fingerprint = self._generate_certificate_fingerprint("tmpcert.pem")
        linux_config = azure.servicemanagement.LinuxConfigurationSet(name, username, password, True)
        linux_config.ssh.public_keys.public_keys.append(PublicKey(fingerprint,"/home/%s/.ssh/authorized_keys" % username))
        if user_data:
           linux_config.custom_data = open(user_data,'r').read()
           
        os_hd = azure.servicemanagement.OSVirtualHardDisk(image_name, media_link + name+ ".vhd")
        configuration_set = ConfigurationSet()
        
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
   
    
    def _exists_storage(self):
       for storage in self.sms.list_storage_accounts():
          if storage.service_name == self.service_name:
             return True
       return False
    
    
    def _create_storage(self):
        """
        ' Creates a new storage
        ' name Storage name.
        ' location location where the storage will be created.
        """
        def failure_callback(elapsed, ex):
           if "Time out" in ex.message:
              raise BaseException("Timeout creating storage")
           
        if not self._exists_storage():
            result = self.sms.create_storage_account(self.service_name, self.service_name, self.service_name, self.service_name)
            self.sms.wait_for_operation_status(result.request_id,failure_callback=failure_callback)
            self.logger.info("Storage %s created!" % self.service_name)
        else:
            self.logger.info("Storage %s already exists, skipping storage creation" % self.service_name)
        return self.sms.get_storage_account_properties(self.service_name)
        
        

    def _delete_storage(self):
        """
        ' Deletes storage
        ' name Name of the storage to delete
        """
        self.sms.delete_storage_account(self.service_name)
        
        
    #====================== Host Services operations ==================================================#
          
    def list_services(self):
        """
        ' Return a list of hosts
        """
        services = []
        for service in self.sms.list_hosted_services():
            services.append({'url':service.url, 'name':service.service_name})
        return services

              
    def _create_service(self):
        """
        Creates a new host
        name Name of the host
        affinity_group affinity_group where host will be created
        If create_affinity_group is True and the affinity_group does not exists, it will be created
        Location is only necesary if create_affinity_group is True
        """
        if not self.sms.check_hosted_service_name_availability(self.service_name).result:
            raise Exception("Name host is not available")
         
        request = self.sms.create_hosted_service(self.service_name,
                                                 self.service_name,
                                                 self.service_name,
                                                 affinity_group=self.service_name)
        if request != None:
           self.sms.wait_for_operation_status(request.request_id)
        self.logger.info("Service %s created!" % self.service_name)
        f = open("tempcert.pfx",'r')
        text = f.read()
        data = base64.b64encode(text)
        request = self.sms.add_service_certificate(self.service_name, data, 'pfx', "")
        if request != None:
           try:
               self.sms.wait_for_operation_status(request.request_id)
           except Exception,e:
              raise("Check error " + e)
        self.logger.info("Certificate added to service %s" % self.service_name)
        return True
     
     
    def _delete_service(self):
       self.sms.delete_hosted_service(self.service_name)
   
    #====================== Affinity group operations ==================================================#
       
    def _list_affinity_group(self):
       return self.sms.list_affinity_groups()
             
   
    def _exist_affinity_group(self):
       """
       Checks if an affinity group exists
       """
       affinity_groups = self._list_affinity_group()
       if len(affinity_groups) == 0:
          return False
       
       for affinity_group in affinity_groups:
          if self.service_name == affinity_group.name.lower():
             return True
       return False
    
    
    def _create_affinity_group(self, location="West Europe"):
        """
        Creates new affinity_group
        """
        if self._exist_affinity_group():
           self.logger.info("Affinity %s already exists, skipping creation" % self.service_name)
           return True
        
        request = self.sms.create_affinity_group(self.service_name, self.service_name, location)
        if request != None:
           try:
              self.sms.wait_for_operation_status(request.request_id)
              self.logger.info("Affinity %s created!" % self.service_name)
           except:
              return True
        return True
     
    
    def _delete_affinity_group(self):
       try:
           self.sms.delete_affinity_group(self.service_name)
       except Exception,e:
           print e
       finally:
          return True

class Server(object):
   
   def __init__(self, role):
      self.name = role.role_name
      try:
          self.state = role.power_state
      except Exception:
          self.state = 'Starting'
      try:
          self.details = role.instance_state_details
      except Exception:
         self.details = ''
      try:
         self.error_code = role.instance_error_code
      except Exception:
         self.error_code = ''
      try:
         self.ip = role.ip_address
      except Exception:
         self.ip = '0.0.0.0'
 
#image = "0b11de9248dd4d87b18621318e037d37__RightImage-CentOS-6.5-x64-v14.1.5.1"
#media_link ="https://testcern.blob.core.windows.net/images/"
#az = Azure('/home/lvillazo/Downloads/vs.publishsettings','testcern')

#for vm in az.list_vms('testcern'):
#   az.delete_vm('testcern', vm['name'])


#for vm in az.list_vms():
#   print vm

