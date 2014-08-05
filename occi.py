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
      result_command = self.process_std(command)
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
      result = ''
      err_result = ''
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
         servers.append(self.describe(server))
      return servers
   
   
   def describe(self, name):
      description = self.occi._describe(name)
      id = description['attributes']['occi']['core']['id']
      hostname = description['attributes']['occi']['compute']['hostname']
      status = description['attributes']['occi']['compute']['state']
      ip = description['links'][0]['attributes']['occi']['networkinterface']['address']
      
      #check os and flavor
      os = description['mixins'][1]
      os = self.occi.images.describe(os[os.index('#')+1:])
      
      flavor = description['mixins'][0]
      flavor = self.occi.flavors.describe(flavor[flavor.index('#')+1:])
      
      return Server(self.occi, name, id, hostname, status, ip, os, flavor)
      
      
   def create(self, name, image, flavor, meta={}, user_data=None, key_name=None ):
      meta['occi.core.title'] = name
      meta['vcycle.starttime'] = datetime.now()
      meta['vcycle.name'] = 'test'
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
      self.created = datetime.now()
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
                'flavor':self.flavor}
      return json.dumps(result)
      


#occi = Occi("https://prisma-cloud.ba.infn.it:8787/",user_cred='/tmp/x509up_u0')
#print occi.servers.list()
#print occi.servers.describe('https://prisma-cloud.ba.infn.it:8787/compute/49f326ea-495e-4852-9e0e-74eae063565a'.strip())

#print occi.flavors.list()
#print occi.images.find('Cern')

#server = occi.servers.create('lvillazoVM','5364f77a-e1cb-4a6c-862e-96dc79c4ef67', 'm1-medium',user_data='file://$PWD/tmpfedcloud.login')
#print server
#server.delete()
#print occi.servers.delete('https://cloud.ifca.es:8787/compute/3055d960-5ea3-4616-9431-084d862fa7f8')
#print occi.compute.describe('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')
#print occi.compute.delete('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')


#occi.delete('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')