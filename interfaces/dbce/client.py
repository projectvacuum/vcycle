import requests
from requests.auth import HTTPBasicAuth
import json
from models.models import * 
import abc

domain_name = ""
username = ''
password = ''

def _error_400(response=None):
   message = 'Problem with request syntax'
   if not response is None:
      message = "%s : %s" % (message, response)
   raise Exception(message)
   
   
def _error_401(response=None):
   raise Exception('No username or password provided/ Username or password is wrong')
   
   
def _error_404(response=None):
   raise Exception('No resource found')
   
   
def _error_500(response=None):
   raise Exception('An error ocurred when performing the operation')


class Op():
   status = {400:_error_400,401:_error_401,404:_error_404,500:_error_500}
   
   @abc.abstractmethod
   def list_result(self, result):
      pass
   
   def list(self, provider = None):
      params = {}
      if not provider is None:
         params['provider-location-uid'] = self.dbce.providers[provider]['id']
      self.status[200] = self.list_result
      request = self.dbce.execute(self.url, params)
      return self.status[request.status_code](request.json())
      

class DBCE():
   
   def __init__(self, endpoint, username, password):
      self.endpoint = endpoint
      self.username = username
      self.password = password
      self.providers = {}
      
      self.machine = MachineOp(self)
      self.machine_template = MachineTemplateOp(self)
      self.network = NetworkOp(self)
      self._load_providers()
      print self.providers
   
   def _load_providers(self):
      request = self.execute("%s/%s" % (self.endpoint,'cxf/multicloud/providerLocations/'))
      for providerLocation in request.json()['providerLocations']:
         id = providerLocation['uid']
         r_providers = self.execute(providerLocation['provider']['href'].replace('cxf','ui'))
         name = r_providers.json()['name']
         self.providers[name] = {'id':id}
         pool_endpoint = self.endpoint[:self.endpoint.rfind(":")].replace('http','https')
         pools = self.execute("%s/%s" % (pool_endpoint, '/ui/capacity/capacityPools'))
         for pool in pools.json()['capacityPools']:
            if pool['providerLocation']['href'] == providerLocation['id']:
               self.providers[name]['pool'] = pool['capacities']['href'][:pool['capacities']['href'].rfind('/')]
               break
      
      
   def execute(self, url, params=None, method='GET', data=None):
      headers = {'Accept':'application/json','Content-Type':'application/json'}
      if method == 'GET':
         return requests.get(url,
                          headers=headers,
                          params=params, 
                          verify=False, 
                          auth=HTTPBasicAuth(self.username,self.password))
      elif method == 'POST':
         return requests.post(url,
                          headers=headers,
                          params=params, 
                          verify=False, 
                          data=data,
                          auth=HTTPBasicAuth(self.username,self.password))
      elif method == 'PUT':
        return requests.put(url,
                          headers=headers,
                          params=params, 
                          verify=False, 
                          auth=HTTPBasicAuth(self.username,self.password))
      elif method == 'DELETE':
         return requests.delete(url,
                          headers=headers,
                          params=params, 
                          verify=False, 
                          auth=HTTPBasicAuth(self.username,self.password))


class MachineOp(Op):
   
   def __init__(self, dbce):
      self.dbce = dbce
      self.url = "%s/cxf/iaas/machines" % dbce.endpoint
      self.status = {400:_error_400,401:_error_401,404:_error_404,500:_error_500}
   
   
   def list_result(self,response):
         servers = []
         for machine in response['machines']:
            server = Machine(machine['id'], machine['name'],
                            machine['description'], machine['created'], machine['updated'],
                            machine['state'], machine['cpu'], machine['memory'], machine['disks'],
                            machine['networkInterfaces'])
            servers.append(server)
         return servers
         
   
   def describe(self, machine_id, provider):
      def _status_200(machine):
         return Machine( machine['id'], machine['name'],
                            machine['description'], machine['created'], machine['updated'],
                            machine['state'], machine['cpu'], machine['memory'], machine['disks'],
                            machine['networkInterfaces'])
      
      
      self.status[200] = _status_200
      params = {'provider-location-uid': self.dbce.providers[provider]['id']}
      request = self.dbce.execute("%s/%s" %(self.url, machine_id),params)
      return self.status[request.status_code](request.json())      
            
   
   def delete(self, machine_id, provider = None):
      def _status_202():
         pass
      
      params = {}
      if not provider is None:
         params['provider-location-uid'] = self.dbce.providers[provider]['id']
      self.status[202] = _status_202
      request = self.dbce.execute("%s/%s" %(self.url, machine_id),params=params,method='DELETE')
      return self.status[request.status_code]()     
    
            
   def create(self, name, description, machine_template, provider, key_data = [], network_interface = []):
      def status_415(response):
         print response
      def _status_201(machine):
         return Machine(machine['id'], machine['name'],
                            machine['description'], machine['created'], machine['updated'],
                            machine['state'], machine['cpu'], machine['memory'], machine['disks'],
                            machine['networkInterfaces'],
                            machine['properties'], machine['credentials'])
      
      json_request = {'resourceURI': "http://schemas.dmtf.org/cimi/1/MachineCreate",
                      'name': name,
                      'description': description,
                      'machineTemplate' :  machine_template.json(),
                      'allocatedFrom':{
                           'resourceURI':'http://schemas.zimory.com/cimi/zcap/1/CapacityPool',
                           'href': self.dbce.providers[provider]['pool']},
                      "allocationSpecs":[
                           {"resourceURI":"http://schemas.zimory.com/cimi/zcap/1/AllocationSpec",
                            "aspect":"http://schemas.dmtf.org/cimi/1/aspect/memory",
                            "classOfService":"bronze",
                            "classOfPerformance":"standard"},
                           {"resourceURI":"http://schemas.zimory.com/cimi/zcap/1/AllocationSpec",
                            "aspect":"http://schemas.dmtf.org/cimi/1/aspect/disk",
                            "classOfService":"bronze",
                            "classOfPerformance":"standard"},
                           {"resourceURI":"http://schemas.zimory.com/cimi/zcap/1/AllocationSpec",
                            "aspect":"http://schemas.zimory.com/dbce/1/aspect/compute",
                            "classOfService":"bronze",
                            "classOfPerformance":"standard"}],
                      
                      }
      
      if len(key_data) > 0:
         json_request['credentials'] = {'resourceURI':'http://schemas.dmtf.org/cimi/1/CredentialCollection',
                                        'credentials':[]}
         
         for key in key_data:
            json_request['credentials']['credentials'].append({'resourceURI':'http://schemas.dmtf.org/cimi/1/Credential',
                                               'keyData':key})
      
      self.status[201] = _status_201
      self.status[415] = status_415
      request = self.dbce.execute("%s" % self.url,method='POST',data=json.dumps(json_request))
      return self.status[request.status_code](request.json())
   
   
   def start(self, machine_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      json_request = {'resourceURI': "%s/cimi/1/Action" % self.endpoint ,
                      'action':"%s/cimi/1/action/start"}
      
      self.status[202] = _status_202
      request = requests.post("%s/%s" %(self.url, machine_id), data=json.dumps(json_request))
      return self.status[request.status_code]()
                              
   
   def stop(self, machine_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      json_request = {'resourceURI': "%s/cimi/1/Action" % self.endpoint ,
                      'action':"%s/cimi/1/action/stop"}
      
      self.status[202] = _status_202
      request = requests.post("%s/%s" %(self.url, machine_id), data=json.dumps(json_request))
      return self.status[request.status_code]()
   
   
   def update(self, machine_id, name, machine_configuration):
      def _status_202():
         return "Request has been successfully accepted"
      
      json_request = {'resourceURI': "%s/cimi/1/Action" % self.endpoint ,
                      'action':"%s/cimi/1/action/update",
                      'name': name,
                      'machineConfiguration': machine_configuration}
      
      self.status[202] = _status_202
      request = requests.post("%s/%s" %(self.url, machine_id), data=json.dumps(json_request))
      return self.status[request.status_code]()
   
   
   def attach_network(self, machine_id, network_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      self.status[202] = _status_202
      request = requests.put("%s/%s/networkInterfaces/%s" %(self.url, machine_id, network_id))
      return self.status[request.status_code]()
   
   
   def detaches_network(self, machine_id, network_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      self.status[202] = _status_202
      request = requests.delete("%s/%s/networkInterfaces/%s" %(self.url, machine_id, network_id))
      return self.status[request.status_code]()
   
   
   def add_network_address(self, machine_id, network_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      json_request = {'resourceURI': "%s/cimi/1/MachineNetworkInterfaceAddress" % self.endpoint ,
                      'address':{
                                 'resourceURI':'%s/cimi/1/Address' % self.endpoint,
                                 'network':{
                                            'resourceURI':'%s/cimi/1/Network',
                                            'networkType':network_id
                                            }
                                 }
                      }
      
      self.status[202] = _status_202
      request = requests.post("%s/%s/networkInterfaces/%s/address" %(self.url, machine_id, network_id), data=json_request)
      return self.status[request.status_code]()
   
   
   def delete_network_address(self, machine_id, network_id):
      def _status_202():
         return "Request has been successfully accepted"
      
      self.status[202] = _status_202
      request = requests.delete("%s/%s/networkInterfaces/%s/address" %(self.url, machine_id, network_id))
      return self.status[request.status_code]()
       
       
   
class MachineTemplateOp(Op):
      
   def __init__(self , dbce):
      self.dbce = dbce
      self.status = {400:_error_400,401:_error_401,404:_error_404,500:_error_500}
      self.url = "%s/cxf/iaas/machineTemplates" % dbce.endpoint

   
   def list_result(self,response):
         templates = []
         for machine_templates in response['machineTemplates']:
             templates.append(MachineTemplate.create(machine_templates))
         return templates
      
  
   def find(self, name, flavor=None, provider = None):
      templates = []
      for t in self.list(provider):
         if t.machine_image['name'].upper().find(name.upper()) >= 0:
            if t.machine_config['name'] == flavor:
               templates.append(t)
      return templates 
   
   
   def describe(self, template_id):
      def _status_200(json_response):
         return MachineTemplate.create(json_response)
            
      self.status[200] = _status_200
      request = requests.get("%s/%s" % (self.url, template_id))
      return self.status[request.status_code](request.json())



class NetworkOp(Op):
   
   def __init__(self, dbce):
      self.dbce = dbce
      self.url = "%s/cxf/iaas/networks" % dbce.endpoint
   
   
   def list_result(self, provider_location_uid = None):
      def _status_200(result):
         networks = []
         for network in result['networks']:
            pass
         return networks

   
   def create(self, name, cidr, allocation_pools, ip_version, enable_dhcp):
      def _status_201():
         pass
      
      json_request = {'resourceURI':'http://schemas.dmtf.org/cimi/1/NetworkCreate',
                      'name':name,
                      'networkTemplate': NetworkTemplate(cidr,{'allocation_pools':allocation_pools,
                                                               'ip_version':ip_version,
                                                               'enable_dhcp':enable_dhcp})
                      }
      self.status[201] = _status_201
      headers = {'content-type': 'application/json'}
      request = requests.get(self.url, data=json_request, headers=headers)
      return self.status[request.status_code]()
   

   def describe(self, network_id):
      def status_200(json_response):
         return Network.create(json_response)
      
      self.status[200] = status_200
      request = requests.get("%s/%s" % (self.url,network_id))
      return self.status[request.status_code]()
   
   
   def update(self, network_id, name, description, created, updated, state, network_type):
      def status_200(json_response):
         return "Request has been successfully accepted"
      
      network = Network(network_id, name, description, created, updated, state, network_type)
      self.status[200] = status_200
      request = requests.put("%s/%s" % (self.url,network_id), data=json.dumps(network))
      return self.status[request.status_code]()
   
   
   def delete(self, network_id):
      def status_202( ):
         return "Request has been successfully accepted"
      
      self.status[202] = status_202
      request = requests.delete("%s/%s" % (self.url,network_id))
      return self.status[request.status_code]()
   
   
      



      


      


      
