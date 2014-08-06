from subprocess import Popen, PIPE 
import json
from datetime import datetime
import logging


class Occi():

   command_credentials = ''
   
   def __init__(self, endpoint, auth='x509', voms=True, user_cred=None):
      self.flavors = Flavor(self)
      self.images = Image(self)
      self.servers = Compute(self)
      self.network = Network(self)
      self.storage = Storage(self)
      
      self.endpoint = endpoint
      self.command_credentials = " --auth %s" % auth
      
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
      " --context user_data=%s " % (self.endpoint, mix_os, mix_resource, user_data)
      
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
      list = self.list()
      for value in list:
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
      list = self.list()
      for value in list:
         description = self.describe(value)
         if name in description['title']:
            resources.append(description)
      return resources
     

class Compute():
   
   def __init__(self, occi):
      self.occi = occi
      
      
   def list(self):
      servers = []
      list_servers = self.occi._list('compute')
      for server in list_servers:
         try:
            servers.append(self.describe(server))
         except Exception,e:
            logging.error(e)
      return servers
   
   
   def describe(self, name):
      description = self.occi._describe(name)
      id = description['attributes']['occi']['core']['id']
      hostname = description['attributes']['occi']['compute']['hostname']
      status = description['attributes']['occi']['compute']['state']
      if not status in ['inactive','error','stopped']:
         ip = description['links'][0]['attributes']['occi']['networkinterface']['address']
      else:
         ip = 'None'
      
      #check os and flavor
      os = description['mixins'][1]
      os = self.occi.images.describe(os[os.index('#')+1:])
      
      flavor = description['mixins'][0]
      flavor = self.occi.flavors.describe(flavor[flavor.index('#')+1:])
      
      return Server(self.occi, name, id, hostname, status, ip, os, flavor)
      
      
   def create(self, name, image, flavor, meta={}, user_data=None, key_name=None ):
      meta = {}
      meta['occi.core.title'] = name
      result = self.occi._create(image, flavor, user_data, meta)
      return self.describe(result)
   
   
   def delete(self, resource):
      return self.occi._delete(resource)

   

class Network():
   
   def __init__(self, occi):
      self.occi = occi
      
   def list(self):
      return self.occi._list('network')



class Storage():
   
   def __init__(self, occi):
      self.occi = occi
      
   def list(self):
      return self.occi._list('storage')
   
   
class Server():
   
   created = None
   updated = None
   
   def __init__(self, occi, resource, id, name, status, ip, os, flavor):
      self.occi = occi
      self.resource = resource
      self.id = id
      self.name = name
      self.status = status
      self.ip = ip
      self.os = os
      self.flavor = flavor
      if 'vcycle' in name:
         self.created = name[len('vcycle-'):]
      else:
         self.created = None
      self.updated = self.created
      
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
                'created':self.created}
      return json.dumps(result)
      


#occi = Occi("https://egi-cloud.pd.infn.it:8787/",user_cred='/tmp/x509up_u0')

#print occi.flavors.list()
#for im in occi.images.list():
#   print occi.images.describe(im)
#print occi.images.find('Cern')
#occi --endpoint https://cloud.cesga.es:3202/ --action create --resource compute --mixin os_tpl#uuid_fedcloud_cernvm_virtual_machine_publicimagelist_267 --mixin resource_tpl#medium --context user_data=file:///var/lib/vcycle/user_data/lvillazovm2:cern-prod_cloud --attribute occi.core.title='vcycle-1407334781'  --auth x509 --user-cred /tmp/x509up_u0 --voms
#server = occi.servers.create('lvillazoVM','c64908ae-86ca-4be3-bcb3-6077aa6b5d32', 'hpc',user_data='file://$PWD/tmpfedcloud.login')
#print server
#server.delete()
#server = occi.servers.create('lvillazoVM','uuid_fedcloud_cernvm_virtual_machine_publicimagelist_267','medium',user_data='file://$PWD/tmpfedcloud.login')



