import json

class Credential():
   keydata = None
   
   def __init__(self, keydata):
      self.keydata = keydata


class Disk():
   capacity = None
   
   def __init__(self, capacity):
      self.capacity = capacity
      

class MachineConfig():
   name = None
   id = None
   cpu = None
   memory = None
   disks = None
   compute = None
   
   def __init__(self, id, name, cpu, memory, disks, compute):
      self.id = id
      self.name = name
      self.cpu = cpu
      self.memory = memory
      self.disks = disks
      self.compute = compute
   
   @staticmethod
   def create(dictionary):
      return MachineConfig(dictionary['id'],
                           dictionary['name'],
                           dictionary['cpu'],
                           dictionary['memory'],
                           dictionary['disks'],
                           dictionary['compute'])
      
      
class MachineImage():
   id = None
   name = None
   created = None
   updated = None
   state = None
   
   def __init__(self, id, name, created, updated, state):
      self.id = id
      self.name = name
      self.created = created
      self.updated = updated
      self.state = state
      
   @staticmethod
   def create(dictionary):
      return MachineImage(dictionary['id'],
                          dictionary['name'],
                          dictionary['created'],
                          dictionary['updated'],
                          dictionary['state'])
      

class MachineTemplate():
   id = None
   machine_config = None
   machine_image = None
   credentials = None
   user_data = None
   
   
   def __init__(self, id, machine_config, machine_image, credentials=None):
      self.resource = "http://schemas.dmtf.org/cimi/1/MachineTemplate"
      self.id = id
      self.machine_config = machine_config
      self.machine_image = machine_image
      self.credentials = credentials
      
   
   @staticmethod
   def create(dictionary):
      if 'credentials' in dictionary:
         return MachineTemplate(dictionary['id'],
                                dictionary['machineConfig'],
                                dictionary['machineImage'],
                                dictionary['credentials'])
      else:
         return MachineTemplate(dictionary['id'],
                                dictionary['machineConfig'],
                                dictionary['machineImage'])
         
   def json(self):
      dict = {'resourceURI':self.resource,
              'id':self.id,
              'machineConfig':{'resourceURI':'http://schemas.dmtf.org/cimi/1/MachineConfiguration',
                               'href': self.machine_config['id']
                               },
              'machineImage':{'resourceURI':'http://schemas.dmtf.org/cimi/1/MachineImage',
                              'href': self.machine_image['id']},
              "networkInterfaces":[{'network':{'href':'https://ec2-54-170-149-5.eu-west-1.compute.amazonaws.com/cxf/iaas/networks/a0c6fed4-dd75-4973-867b-8c9f31e77406'}},
                                   {"network":{"networkType":"PUBLIC"}}]
              }
      if not self.credentials is None:
         dict['credentials'] = self.credentials
      
      if not self.user_data is None:
         dict['userData'] = self.user_data
      return dict
   
     
      
   

class Network():
   
   def __init__(self, id, name, description, created, updated, state, network_type):
      self.id = id
      self.name = name
      self.description = description
      self.created = created
      self.updated = updated
      self.state = state
      self.network_type = network_type
   

   @staticmethod
   def create(dictionary):
      return Network(dictionary['id'], dictionary['name'], dictionary['description'],
                     dictionary['created'], dictionary['updated'], dictionary['state'],
                     dictionary['network_type'])


class NetworkTemplate():
      
   def __init__(self,name, cidr, properties={}):
      self.resource = 'http://schemas.dmtf.org/cimi/1/NetworkTemplate'
      self.name = name
      self.properties['cidr'] = cidr
      
      if 'allocation_pools' in properties:
         self.properties['cidr'] = cidr
      if 'gateway_ip' in properties and not properties['gateway_ip'] is None:
         self.properties['gateway_ip'] = properties['gateway_ip']
      if 'ip_version' in properties and not properties['ip_version'] is None:
         self.properties['ip_version'] = properties['ip_version']
      if 'enable_dhcp' in properties and not properties['enable_dhcp'] is None:
         self.properties['enable_dhcp'] = properties['enable_dhcp']
         
         
class Machine():
   
   def __init__(self, id, name, description,
                created, updated, state, cpu, memory, disks,
                network_interfaces, properties = None, credentials = None):
      self.resourceURI = 'http://schemas.dmtf.org/cimi/1/Machine'
      self.id = id[id.rfind('/')+1:]
      self.name = name
      self.description = description
      self.created = created
      self.updated = updated
      self.status = state
      self.cpu = cpu
      self.memory = memory
      self.disks = disks
      if len(network_interfaces['machineNetworkInterfaces']) > 0:
         self.network_interfaces = network_interfaces['machineNetworkInterfaces'][0]['addresses']['machineNetworkInterfaceAddresses']
      else:
         self.network_interfaces = []
      self.properties = properties
      self.credentials = credentials
      
   def __repr__(self):
      return "%s %s %s" % (self.id, self.name, self.status)