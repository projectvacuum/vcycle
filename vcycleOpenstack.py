from vcycleBase import vcycleBase
import os
import VCYCLE
import uuid
import time , calendar
from novaclient.client import Client
class vcycleOpenstack(vcycleBase):
   '''Class to create VMs using OpenStack interface'''
   
   def _create_client(self):
      '''Created a new Openstack client'''
      tenancy = self.tenancy
      if 'proxy' in tenancy:
         import novaclient
         import novaclient.auth_plugin
         novaclient.auth_plugin.discover_auth_systems()
         auth_plugin = novaclient.auth_plugin.load_plugin('voms')
         auth_plugin.opts["x509_user_proxy"] = tenancy['proxy']
         novaClient = novaclient.client.Client('1.1',None,None,project_id=tenancy['tenancy_name'],auth_url=tenancy['url'],
                                               auth_plugin=auth_plugin,auth_system='voms',insecure=True) 
      else:    
         import novaclient 
         novaClient = novaclient.client.Client('1.1', username=tenancy['username'], api_key=tenancy['password'],
                                               project_id=tenancy['tenancy_name'], auth_url=tenancy['url'])
      return novaClient
   
   
   def _servers_list(self):
      '''Returns a list of all servers created and not deleted in the tenancy'''
      serversList = self.client.servers.list(detailed=True)
      return serversList
   
   
   def _retrieve_properties(self, server, vmtypeName):
      '''Returns the server's properties'''
      properties = {}
      properties['createdTime']  = calendar.timegm(time.strptime(server.created, "%Y-%m-%dT%H:%M:%SZ"))
      properties['updatedTime']  = calendar.timegm(time.strptime(server.updated, "%Y-%m-%dT%H:%M:%SZ"))
      
      properties['taskState']  = str(getattr(server, 'OS-EXT-STS:task_state' ))
      properties['powerState'] = str(getattr(server, 'OS-EXT-STS:power_state'))

      try:
        properties['launchedTime'] = calendar.timegm(time.strptime(str(getattr(server,'OS-SRV-USG:launched_at')).split('.')[0], "%Y-%m-%dT%H:%M:%S"))
        properties['startTime']    = properties['launchedTime']
      except:
        properties['launchedTime'] = None
        properties['startTime']    = properties['createdTime']        
      else:        
        if not os.path.isfile('/var/lib/vcycle/machines/' + server.name + '/launched'):
          VCYCLE.createFile('/var/lib/vcycle/machines/' + server.name + '/launched', str(properties['launchedTime']), 0600)
        
      try:
        properties['ip'] = str(getattr(server, 'addresses')['CERN_NETWORK'][0]['addr'])
      except:
        properties['ip'] = '0.0.0.0'
        
      try:
        properties['heartbeatTime'] = int(os.stat('/var/lib/vcycle/machines/' + server.name + '/machineoutputs/vm-heartbeat').st_ctime)
        properties['heartbeatStr'] = str(int(time.time() - properties['heartbeatTime'])) + 's'
      except:
        properties['heartbeatTime'] = None
        properties['heartbeatStr'] = '-'
      
      VCYCLE.logLine(self.tenancyName, server.name + ' ' + 
              (vmtypeName + '                  ')[:16] + 
              (properties['ip'] + '            ')[:16] + 
              (server.status + '       ')[:8] + 
              (properties['taskState'] + '               ')[:13] +
              properties['powerState'] + ' ' +
              server.created + 
              ' to ' + 
              server.updated + ' ' +
              ('%5.2f' % ((time.time() - properties['startTime'])/3600.0)) + ' ' +
              properties['heartbeatStr'])
      return properties
      
   
   def _update_properties(self, server, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
      '''Updates the server's properties'''
      tenancy = self.tenancy
      tenancyName = self.tenancyName
      
      if server.status == 'SHUTOFF' and (properties['updatedTime'] - properties['startTime']) < tenancy['vmtypes'][vmtypeName]['fizzle_seconds']:
        VCYCLE.logLine(self.tenancyName, server.name + ' was a fizzle! ' + str(properties['updatedTime'] - properties['startTime']) + ' seconds')
        try:
          VCYCLE.lastFizzles[tenancyName][vmtypeName] = properties['updatedTime']
        except:
          # In case vmtype removed from configuration while VMs still existed
          pass

      if server.status == 'ACTIVE' and properties['powerState'] == '1':
        # These ones are running properly
        totalRunning += 1
        
        if vmtypeName not in runningPerVmtype:
          runningPerVmtype[vmtypeName]  = 1
        else:
          runningPerVmtype[vmtypeName] += 1

      # These ones are starting/running, but not yet passed tenancy['vmtypes'][vmtypeName]['fizzle_seconds']
      if ((server.status == 'ACTIVE' or 
           server.status == 'BUILD') and 
          ((int(time.time()) - properties['startTime']) < tenancy['vmtypes'][vmtypeName]['fizzle_seconds'])):
          
        if vmtypeName not in notPassedFizzleSeconds:
          notPassedFizzleSeconds[vmtypeName]  = 1
        else:
          notPassedFizzleSeconds[vmtypeName] += 1
      
      return totalRunning
      
      
   def _describe(self, server):
      '''Returns the descripion of a server. This method is empty because when the server is created,
      Openstack returns directly all the vm description'''
      pass
      
      
   def _delete(self, server, vmtypeName, properties):
      '''Deletes a server'''
      tenancy = self.tenancy
      if server.status  == 'BUILD':
         return False
      
      if ( 
           (
             # We always delete if in SHUTOFF state and not transitioning
             server.status == 'SHUTOFF' and properties['taskState'] == 'None'
           ) 
           or 
           (
             # We always delete if in ERROR state and not transitioning
             server.status == 'ERROR' and properties['taskState'] == 'None'
           ) 
           or 
           (
             # ACTIVE gets deleted if longer than max VM lifetime 
             server.status == 'ACTIVE' and properties['taskState'] == 'None' and ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds'])
           )
           or 
           (
             # ACTIVE gets deleted if heartbeat defined in configuration but not updated by the VM
             'heartbeat_file' in tenancy['vmtypes'][vmtypeName] and
             'heartbeat_seconds' in tenancy['vmtypes'][vmtypeName] and
             server.status == 'ACTIVE' and properties['taskState'] == 'None' and 
             ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds']) and
             (
              (properties['heartbeatTime'] is None) or 
              ((int(time.time()) - properties['heartbeatTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds'])
             )              
           )
           or
           (
             (
               # Transitioning states ('deleting' etc) get deleted ...
               server.status  == 'SHUTOFF' or
               server.status  == 'ERROR'   or 
               server.status  == 'DELETED' or          
               (server.status == 'ACTIVE' and properties['powerState'] != '1')
             )
             and
             (
               # ... but only if this has been for a while
               properties['updatedTime'] < int(time.time()) - 900
             )             
           )
         ):
        VCYCLE.logLine(self.tenancyName, 'Deleting ' + server.name)
        try:
          server.delete()
          return True
        except Exception as e:
          VCYCLE.logLine(self.tenancyName, 'Delete ' + server.name + ' fails with ' + str(e))
      return False
          
   def _server_name(self, name=None):
      '''Returns the server name'''
      return 'vcycle-' + str(uuid.uuid4())
   
   
   def _create_machine(self, serverName, vmtypeName, proxy=False):
      '''Creates a new VM using Openstack interface'''
      tenancyName = self.tenancyName
      meta={ 'cern-services'   : 'false',
             'machinefeatures' : 'http://'  + os.uname()[1] + '/' + serverName + '/machinefeatures',
             'jobfeatures'     : 'http://'  + os.uname()[1] + '/' + serverName + '/jobfeatures',
             'machineoutputs'  : 'https://' + os.uname()[1] + '/' + serverName + '/machineoutputs'
           }
      if proxy :
         return self.client.servers.create(serverName, 
               self.client.images.find(name=VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['image_name']),
               self.client.flavors.find(name=VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['flavor_name']), 
               meta=meta, 
               userdata=open('/var/lib/vcycle/user_data/' + tenancyName + ':' + vmtypeName, 'r').read())
      else:
         try:
            net = self.client.networks.find(label="net01")
            nics = [{'net-id': net.id}]
         except Exception:
            nics = []
         return self.client.servers.create(serverName, 
                self.client.images.find(name=VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['image_name']),
                self.client.flavors.find(name=VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['flavor_name']),
                meta=meta, 
                nics=nics,
                key_name=VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['root_key_name'],
                userdata=open('/var/lib/vcycle/user_data/' + tenancyName + ':' + vmtypeName, 'r').read())

