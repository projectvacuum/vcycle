__author__ = 'Luis Villazon Esteban'

from azure.servicemanagement import *
from subprocess import Popen, PIPE 
import base64
import uuid

class Azure():
   
    def __init__(self, publish_settings):
        subscription_id = get_certificate_from_publish_settings(
            publish_settings_path=publish_settings,
            path_to_write_certificate='tmpcert.pem'
        )
        self.sms = ServiceManagementService(subscription_id, 'tmpcert.pem')
        self._generate_pfx('tmpcert.pem')



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
              
          
    def create_virtual_machine(self, name, affinity, location='West Europe', username=None, password=None,
                               image_name=None, media_link=None, user_data=None, ignore_host=True):
        """Created a new virtual machine
        ' name Virtual machine name
        ' username , username to use to connect via SSH to the VM
        ' password password to connect via SSH to the VM
        ' image_name Name of the image to use.
        ' media_link link to storage the VM
        :type image_name: object
        """
        if image_name is None:
            raise Exception("Image name is mandatory")
        if media_link is None:
            raise Exception("Media link is mandatory")

        self._create_affinity_group(affinity,location)
        try:
            self._create_service(name, affinity)
        except Exception,e:
           if not ignore_host:
              raise e
           
        self._create_deployment(name,affinity, username=username, password=password, user_data=user_data,
                        image_name=image_name, media_link=media_link)

    
           
    def _create_deployment(self, service_name, affinity_group, image_name=None,
                   media_link=None, user_data=None, username=None, password=None):
        """
        "Creates VM
        ' name VM name
        ' username username to use to connect via SSH to the VM
        ' password password to use to connect via SSH to the VM
        ' location location where the VM will be allocated.
        ' image_name Name of the image to use
        ' media_link link to store the VM
        """
        vm_name = uuid.uuid4()
        values = self._common_for_deployments_and_roles(vm_name, username, password, user_data, image_name, media_link)
        response = self.sms.create_virtual_machine_deployment(service_name, service_name, 'production', service_name, vm_name, values['linux_config'],
                                                   values['os_hd'],
                                                   availability_set_name=affinity_group,
                                                   network_config=values['configuration_set'],
                                                   role_size='Small')
        self.sms.wait_for_operation_status(response.request_id)


    def _create_role(self, service_name, image_name=None,
                     media_link=None, user_data=None, username=None, password=None):
        
        vm_name = uuid.uuid4()
        values = self._common_for_deployments_and_roles(vm_name, username, password, user_data, image_name, media_link)
        response = self.sms.add_role(service_name, service_name,
                                     vm_name, values['linux_config'],
                                     values['os_hd'],network_config=values['configuration_set'],
                                    role_size='Small')
        self.sms.wait_for_operation_status(response.request_id)
        
   
    def _common_for_deployments_and_roles(self, name, username, password, user_data, image_name, media_link):
        fingerprint = self._generate_certificate_fingerprint("tmpcert.pem")
        linux_config = azure.servicemanagement.LinuxConfigurationSet(name, username, password, True)
        linux_config.ssh.public_keys.public_keys.append(PublicKey(fingerprint,"/home/%s/.ssh/authorized_keys" % username))
        if user_data:
           linux_config.custom_data = open(user_data,'r').read()
           
        os_hd = azure.servicemanagement.OSVirtualHardDisk(image_name, media_link + name+ ".vhd")
        configuration_set = ConfigurationSet()
        
        configuration_set.input_endpoints.input_endpoints.append(
                ConfigurationSetInputEndpoint(name=u'SSH', protocol=u'TCP', port=u'22', local_port=u'22'))
        
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
    
    
    
    def _create_storage(self, name, location="North Europe"):
        """
        ' Creates a new storage
        ' name Storage name.
        ' location location where the storage will be created.
        """
        result = self.sms.create_storage_account(name, name, name, location=location)


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
         
        request_id = self.sms.create_hosted_service(name, name, name, affinity_group=affinity_group)
        if request_id != None:
           self.sms.wait_for_operation_status(request_id.request_id)
        f = open("tempcert.pfx",'r')
        text = f.read()
        data = base64.b64encode(text)
        request_id = self.sms.add_service_certificate(name, data, 'pfx', "")
        if request_id != None:
           try:
               self.sms.wait_for_operation_status(request_id.request_id)
           except Exception,e:
              raise("Check error " + e)
        return True
   
   
       
    def _get_affinity_group(self,name):
       affinity_group = self.sms.get_affinity_group_properties(name)
       for hosted_services in affinity_group.hosted_services:
          print hosted_services.deployments
          
    
    
    def _exist_affinity_group(self, name):
       """
       Checks if an affinity group exists
       """
       affinity_groups = self.sms.list_affinity_groups()
       if len(affinity_groups) == 0:
          return False
       
       for affinity_group in affinity_groups:
          if name == affinity_group.name:
             return True
       return False
    
    
    def _create_affinity_group(self, name, location="West Europe"):
        """
        Creates new affinity_group
        """
        if self._exist_affinity_group(name):
           return True
        
        request = self.sms.create_affinity_group(name, name, location)
        if request != None:
           try:
              self.sms.wait_for_operation_status(request.request_id)
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


image = "0b11de9248dd4d87b18621318e037d37__RightImage-CentOS-6.5-x64-v14.1.5.1"
media_link ="https://testcern.blob.core.windows.net/images/"
az = Azure('/home/lvillazo/Downloads/vs.publishsettings')
print az.list_services()
print az.delete_vm('testCernHost','test2')
#az._create_role("testCernHost", '449595e9-ec50-4d80-b656-87f9baea654c',image, media_link,"test.user_data","luis","Espronceda1985$")
#az.get_os()
#print az.list_deployments()
#az.delete_deployment("test2lv")
#az.create_virtual_machine("xxxx","xxx","xxxx", image, media_link, "test.user_data")
