__author__ = 'Luis'

from azure.servicemanagement import *
from subprocess import Popen, PIPE 
import base64

class Azure():
   
    def __init__(self, publish_settings):
        subscription_id = get_certificate_from_publish_settings(
            publish_settings_path=publish_settings,
            path_to_write_certificate='tmpcert.pem'
        )
        self.sms = ServiceManagementService(subscription_id, 'tmpcert.pem')
        self._generate_pfx('tmpcert.pem')


    def create_virtual_machine(self, name, username=None, password=None,
                               image_name=None, media_link=None, user_data=None):
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

        self._create_host(name)
        self._create_vm(name, username=username, password=password, user_data=user_data,
                        image_name=image_name, media_link=media_link)


    def _create_vm(self, name=None, location='North Europe', image_name=None,
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
        def remove_service(elapsed, ex):
           self.sms.delete_hosted_service(name)
           raise Exception("Error creating VM")
           
        fingerprint = self._generate_certificate_fingerprint("tmpcert.pem")
        linux_config = azure.servicemanagement.LinuxConfigurationSet(name, username, password, True)
        linux_config.ssh.public_keys.public_keys.append(PublicKey(fingerprint,"/home/%s/.ssh/authorized_keys" % username))
        if user_data:
           linux_config.custom_data = open(user_data,'r').read()
           
        os_hd = azure.servicemanagement.OSVirtualHardDisk(image_name, media_link + name+ ".vhd")
        configuration_set = ConfigurationSet()
        
        configuration_set.input_endpoints.input_endpoints.append(
                ConfigurationSetInputEndpoint(name=u'SSH', protocol=u'TCP', port=u'22', local_port=u'22'))

        request = self.sms.create_virtual_machine_deployment(name, name, 'production', name, name, linux_config,
                                                   os_hd,
                                                   network_config=configuration_set,
                                                   role_size='Small')
        if request != None:
           self.sms.wait_for_operation_status(request.request_id,sleep_interval=20, failure_callback=remove_service)
        return True


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
          
          
    def get_hosts(self):
        """
        ' Return a list of hosts
        """
        hosts = []
        for s in self.sms.list_hosted_services():
            hosts.append({'url':s.url, 'name':s.service_name})
        return hosts

              
    def get_deployments_from_host(self, name):
       deployments = []
       host = self.sms.get_hosted_service_properties(name, embed_detail=True)
       for deployment in host.deployments:
          roles = []
          for rol in deployment.role_instance_list:
              roles.append({'name':rol.role_name,
                           'instance_name': rol.instance_name,
                           'status':rol.instance_status,
                           'ip':rol.instance_endpoints[0].vip})
          deployments.append(Server.new(self,name, deployment.name,
                                        name, deployment.status, deployment.created_time,
                                        deployment.last_modified_time,roles))
       return deployments
          
    
    def _create_host(self, name, location="North Europe"):
        """
        ' Creates a new host
        ' name Name of the host
        ' location location where host will be created
        """
        if not self.sms.check_hosted_service_name_availability(name).result:
            raise Exception("Name host is not available")
        request_id = self.sms.create_hosted_service(name, name, name, location=location)
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


image = "b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu_DAILY_BUILD-vivid-15_04-amd64-server-20150319.5-en-us-30GB"
media_link ="https://cernvmlv.blob.core.windows.net/images/"
az = Azure('vs.publishsettings')
print az.list_deployments()
#az.delete_deployment("test2lv")
az.create_virtual_machine("xxxx","xxx","xxxx", image, media_link, "test.user_data")
