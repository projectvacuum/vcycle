
from subprocess import Popen, PIPE 
import json
from datetime import datetime


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
      
      result = self.process_std(command)
      return result


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
      print command
      result = ''
      err_result = ''
      pipe=Popen(command,stdout=PIPE, stderr=PIPE, shell=True, close_fds=True)
   
      for line in pipe.stdout.read():
         result +=  line
   
      for line in pipe.stderr.read():
         err_result += line
   
      if len(err_result) > 0:
         raise Exception(err_result)
      if len(err_result) == 0 and len(result) == 0:
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
      title = result_describe[result_describe.index('Flavor:')+len('Flavor:'):result_describe.index('term')]
      term = result_describe[result_describe.index('term:')+len('term:'): result_describe.index('location')]
      location = result_describe[result_describe.index('location:')+len('location:'):]
      location = location[:location.index('#')]
      return {'title': title.strip(),
              'term': term.strip(),
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
      #return_list = []
      return self.occi._list('os_tpl')
      #for image in images_list:
      #   return_list.append(self.describe(image))
      #return return_list
   
   
   def describe(self, name):
      result_describe = self.occi._describe(name)
      try:
         title = result_describe[result_describe.index('Image:')+len('Image: '):result_describe.index('term')]
      except:
         title = name
      term = result_describe[result_describe.index('term:')+len('term:'): result_describe.index('location')]
      location = result_describe[result_describe.index('location:')+len('location:'):]
      location = location[:location.index('#')]
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
      #json_servers = [{"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/49f326ea-495e-4852-9e0e-74eae063565a", "name": "myfirstvm", "ip": "90.147.102.238", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "49f326ea-495e-4852-9e0e-74eae063565a"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/b084830a-7250-4c43-8c4e-b0dfeba77dca", "name": "lvillazovm", "ip": "90.147.102.190", "flavor": "5364f77a-e1cb-4a6c-862e-96dc79c4ef67", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "b084830a-7250-4c43-8c4e-b0dfeba77dca"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/67745a80-00e7-48cc-8c54-d428106bfe1c", "name": "machine-26292edb-8fb4-498e-bb33-ac991588c010", "ip": "90.147.102.247", "flavor": "fcb99b13-cfb2-4ea4-be99-a014d5135e33", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "67745a80-00e7-48cc-8c54-d428106bfe1c"}, {"status": "inactive", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/c99a662c-94e9-44eb-b60d-4bddddb06a2d", "name": "vcycle-a5372d2e-43f5-4f88-b6f2-d736569f5733", "ip": "90.147.102.182", "flavor": "5364f77a-e1cb-4a6c-862e-96dc79c4ef67", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "c99a662c-94e9-44eb-b60d-4bddddb06a2d"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/0437bc56-9d96-47e4-9a1c-74dcd53dc141", "name": "orchestrator-fedcloud-infnbari-78b9116e-6743-4e06-9162-0e69f653f9b3", "ip": "90.147.102.31", "flavor": "ff718bea-602b-4f13-91d2-58d134c45476", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "0437bc56-9d96-47e4-9a1c-74dcd53dc141"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/74d1cc49-3af1-424d-80b8-4d392ff10f00", "name": "myfirstvm", "ip": "90.147.102.236", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "74d1cc49-3af1-424d-80b8-4d392ff10f00"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/e17a69e9-ee3a-4a90-bed4-f9ae38c6e9c1", "name": "vm-recas-school", "ip": "90.147.102.221", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "e17a69e9-ee3a-4a90-bed4-f9ae38c6e9c1"}, {"status": "inactive", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/e13f8953-9488-43c2-852e-70c5a81a6535", "name": "biostif-operational1", "ip": "90.147.102.6", "flavor": "f835d4a5-cb08-4350-b60e-3fdd0dc703a4", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "e13f8953-9488-43c2-852e-70c5a81a6535"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/20ba66af-6352-4ddf-802a-1884f0890b6d", "name": "lvillazovm", "ip": "90.147.102.156", "flavor": "5364f77a-e1cb-4a6c-862e-96dc79c4ef67", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "20ba66af-6352-4ddf-802a-1884f0890b6d"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/2aa72418-1373-4b9b-8155-0c02c477ffb9", "name": "machine-2b601292-54c0-4c6b-915f-ec62685a48bf", "ip": "90.147.102.245", "flavor": "f5031248-a8f5-42d3-9732-28aac265e318", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "2aa72418-1373-4b9b-8155-0c02c477ffb9"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/6766cc08-72d3-4b33-8a93-511f95bf0cc8", "name": "myvm", "ip": "90.147.102.220", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "6766cc08-72d3-4b33-8a93-511f95bf0cc8"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/3f3fb479-1abe-4914-88f3-301933f65b65", "name": "cat-test1", "ip": "90.147.102.186", "flavor": "5364f77a-e1cb-4a6c-862e-96dc79c4ef67", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "3f3fb479-1abe-4914-88f3-301933f65b65"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/2f6d70c6-fb75-4372-9917-ac688b1391ee", "name": "openrefine-test-ubuntu-2", "ip": "90.147.102.41", "flavor": "7cfba655-f692-406f-a659-79b0224290cc", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "2f6d70c6-fb75-4372-9917-ac688b1391ee"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/8123c0fa-26f3-4a46-a8f1-2759b437fedb", "name": "myroccitest", "ip": "90.147.102.213", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "8123c0fa-26f3-4a46-a8f1-2759b437fedb"}, {"status": "inactive", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/b6d0a0b6-9708-4c20-8bfa-fb05255e1e23", "name": "machine-e4ea9f41-4614-4456-9262-a9be74db3611", "ip": "90.147.102.218", "flavor": "5364f77a-e1cb-4a6c-862e-96dc79c4ef67", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "b6d0a0b6-9708-4c20-8bfa-fb05255e1e23"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/c8b266f6-d363-4943-9c70-4c1d234ada76", "name": "myfirstvm", "ip": "90.147.102.198", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "c8b266f6-d363-4943-9c70-4c1d234ada76"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/23facfee-603b-49c8-9e6b-51c90408292f", "name": "machine", "ip": "90.147.102.54", "flavor": "f5031248-a8f5-42d3-9732-28aac265e318", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "23facfee-603b-49c8-9e6b-51c90408292f"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/7e42cc7c-4de7-41fb-aae1-f682e4203279", "name": "myfirstvm", "ip": "90.147.102.2", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "7e42cc7c-4de7-41fb-aae1-f682e4203279"}, {"status": "active", "resource": "https://prisma-cloud.ba.infn.it:8787/compute/d99394a1-09a7-49bc-a81d-8ad3c1afd7b6", "name": "myfirstvm", "ip": "90.147.102.235", "flavor": "72ada03a-5694-4a79-8e7e-069516a31a59", "os": {"term": "os_vms", "location": "/os_vms/", "title": "os_vms"}, "id": "d99394a1-09a7-49bc-a81d-8ad3c1afd7b6"}]
      #for j in json_servers:
      #   servers.append(Server(self.occi,j['resource'],j['id'],j['name'],j['status'],j['ip'],j['os'],j['flavor']))
      for server in list_servers:
         servers.append(self.describe(server))
      return servers
   
   
   def describe(self, name):
      description = self.occi._describe(name)
      id = self.occi._extract_param(description, 'occi.core.id')
      hostname = self.occi._extract_param(description, 'occi.compute.hostname')
      status = self.occi._extract_param(description, 'occi.compute.state')
      ip = self.occi._extract_param(description, 'occi.networkinterface.address')
      
      #check os and flavor
      mixins = description[description.index('Mixins'):]
      mixins = mixins[mixins.index('term:')+len('term:'):]
      os = self.occi.images.describe(mixins[:mixins.index('\n')].strip())
      mixins = mixins[mixins.index('term:')+len('term:'):]
      flavor = mixins[:mixins.index('\n')].strip()
      
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
#print occi.servers.describe('https://prisma-cloud.ba.infn.it:8787/compute/49f326ea-495e-4852-9e0e-74eae063565a')

#print occi.flavors.list()
#print occi.images.find('Cern')

#server = occi.servers.create('lvillazoVM','5364f77a-e1cb-4a6c-862e-96dc79c4ef67', 'm1-medium',user_data='file://$PWD/tmpfedcloud.login')
#print server
#server.delete()
#print occi.servers.delete('https://cloud.ifca.es:8787/compute/3055d960-5ea3-4616-9431-084d862fa7f8')
#print occi.compute.describe('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')
#print occi.compute.delete('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')


#occi.delete('https://prisma-cloud.ba.infn.it:8787/compute/86ae3606-d753-4421-b415-e697b1670879')