from subprocess import Popen, PIPE 
import json
from datetime import datetime
import logging


class Occi():

   command_credentials = ''
   
   def __init__(self, endpoint, auth='x509', voms=True, user_cred=None, username=None, password=None):
      self.flavors = Flavor(self)
      self.images = Image(self)
      self.servers = Compute(self)
      self.endpoint = endpoint
      self.command_credentials = " --auth %s" % auth
      
      if not username is None:
         self.command_credentials = " --username %s" % username
      if not password is None:
         self.command_credentials = " --password %s" % password
      if not user_cred is None:
         self.command_credentials += " --user-cred %s" % user_cred
      if voms:
         self.command_credentials += " --voms"
         

   def _list(self, resource):
      '''List resources. Resource should be : compute, network, storage , os_tpl , resource_tpl'''
      if not resource in ['compute', 'network', 'storage', 'os_tpl', 'resource_tpl']:
         raise Exception('Resource should be : compute, network, storage , os_tpl , resource_tpl') 
      
      command = "occi --endpoint %s --action list --resource %s" % (self.endpoint, resource)
      command += self.command_credentials 
      try:
         result_command = self.process_std(command)
      except Exception,e:
         if "sslv3 alert certificate expired" in e.message:
            raise e
         else:
            logging.error(e)
            return []
      result = []
      for  line in result_command.split('\n'):
         if len(line) > 0:
            result.append(line[line.find('#')+1:])
      return result


   def _describe(self,resource):
      '''Describe a specific resource'''
      command = "occi --endpoint %s --action describe --resource %s" % (self.endpoint, resource)
      command += self.command_credentials
      command += ' --output-format json_extended'
      
      result = self.process_std(command)
      return json.loads(result)[0]


   def _create(self, mix_os, mix_resource, user_data, attributes={}):
      '''Create a new resource'''
      command = "occi --endpoint %s --action create --resource compute --mixin os_tpl#%s --mixin resource_tpl#%s" \
      " --context user_data=\"%s\" " % (self.endpoint, mix_os, mix_resource, user_data)
      
      for key, value in attributes.iteritems():
         command += "--attribute %s='%s' " % (key, value)
      
      command += self.command_credentials
      result = self.process_std(command)
      return result.replace('\n','')


   def _delete(self, resource):
      '''Delete a resource'''
      command = "occi --endpoint %s --action delete --resource %s" % (self.endpoint, resource)
      command += self.command_credentials
      result = None
      try:
         result = self.process_std(command)
      finally:
         return result
      

   def process_std(self,command):
      logging.info(command)
      (result,err_result)=Popen(command,shell=True,stdout=PIPE,stderr=PIPE).communicate()
      
      if len(err_result) > 0:
         raise Exception(err_result)
      if len(err_result) == 0  and len(result) == 0 :
         raise Exception("Unexpected error happens.Check params")
   
      return result


   def _extract_param(self, text, param):
      aux = text[text.find(param+' =')+len(param)+2:]
      return aux[:aux.find('\n')].strip()



class Flavor():
   
   def __init__(self, occi):
      self.occi = occi
      
      
   def list(self):
      return self.occi._list('resource_tpl')
   
   
   def describe(self, name):
      result_describe = self.occi._describe(name)
      title = result_describe['term']
      location = result_describe['location']
      return {'title': title.strip(),
              'term': title.strip(),
              'location':location.strip()
              }
      
      
   def find(self, name=None):
      resources = []
      local_list = self.list()
      for value in local_list:
         if name in self.describe(value)['title']:
            resources.append(value)
      return resources



class Image():
   
   def __init__(self, occi):
      self.occi = occi
      
      
   def list(self):
      return self.occi._list('os_tpl')
   
   
   def describe(self, name):
      result_describe = self.occi._describe(name)
      title = result_describe['title']
      term = result_describe['term']
      location = result_describe['location']
      return {'title': title.strip(),
              'term': term.strip(),
              'location':location.strip()
              }
           
      
   def find(self, name=None):
      resources = []
      local_list = self.list()
      for value in local_list:
         description = self.describe(value)
         if name in description['title']:
            resources.append(description)
      return resources
     


class Compute():
   
   def __init__(self, occi):
      self.occi = occi
      
      
   def list(self, detailed=True, action=None):
      servers = []
      list_servers = self.occi._list('compute')
      for server in list_servers:
         try:
            if detailed:
               ob = self.describe(server)
               servers.append(ob)
               if not action is None:
                  action(ob)
            else:
               servers.append(server)
         except Exception:
            pass
      return servers
   
   
   def describe(self, name):
      error_counter = 0
      def call_describe():
         try:
            return self.occi._describe(name)
         except Exception,e:
            if "Timeout" in e:
               error_counter = error_counter + 1
               if error_counter >= 3:
                  raise e
               return call_describe()
            else:
               raise e
      
      try:
         description = call_describe()
      except Exception:
         return None
      
      vm_id = description['attributes']['occi']['core']['id']
      hostname = description['attributes']['occi']['compute']['hostname']
      status = description['attributes']['occi']['compute']['state']
      if not status in ['inactive','error','stopped'] and len(description['links']) > 0:
         ip = description['links'][0]['attributes']['occi']['networkinterface']['address']
      else:
         ip = 'None'
      
      if "org" in description['attributes'] and "openstack" in description['attributes']['org']:
         console = description['attributes']['org']['openstack']['compute']['console']['vnc']
      else:
         console = None
         
      #check os and flavor
      os = description['mixins'][1]
      os = self.occi.images.describe(os[os.index('#')+1:])
      
      flavor = description['mixins'][0]
      flavor = self.occi.flavors.describe(flavor[flavor.index('#')+1:])
      
      return Server(self.occi, name, vm_id, hostname, status, ip, os, flavor, console)
      
            
   def create(self, name, image, flavor, meta={}, user_data=None, key_name=None ):
      meta = {}
      meta['occi.core.title'] = name
      result = self.occi._create(image, flavor, user_data, meta)
      return self.describe(result)
   
   
   def delete(self, resource):
      return self.occi._delete(resource)

   

class Server():
   
   created = None
   updated = None
   
   def __init__(self, occi, resource, id, name, status, ip, os, flavor, console):
      self.occi = occi
      self.resource = resource
      self.id = id
      self.name = name
      self.status = status
      self.ip = ip
      self.os = os
      self.flavor = flavor
      if 'vcycle' in name:
         aux = name.split('-')
         self.created = aux[len(aux)-1]
      else:
         self.created = None
      self.updated = self.created
      self.console = console
      
   def delete(self):
      return self.occi._delete(self.resource)
   
   
   def __repr__(self):
      result = {'id':self.id,
                'resource':self.resource,
                'name':self.name,
                'status':self.status,
                'ip':self.ip,
                'os': self.os,
                'flavor':self.flavor,
                'created':self.created,
                'console':self.console}
      return json.dumps(result)