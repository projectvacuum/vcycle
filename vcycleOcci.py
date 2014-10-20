from vcycleBase import vcycleBase
import os
import VCYCLE
import time
from occi import Occi

class vcycleOcci(vcycleBase):
   '''Class to create VMs using OCCI interface'''
   
   def _create_client(self):
      '''Created a new OCCI client'''
      tenancy = self.tenancy
      if 'proxy' in tenancy:
          return Occi(tenancy['url'],user_cred=tenancy['proxy'])
      else:
         return Occi(tenancy['url'],
                                username=tenancy['username'],
                                password=tenancy['password'],
                                auth=tenancy['auth'],
                                voms=False)
   
   
   #Return a list of the running servers on the site
   def _servers_list(self):
      '''Returns a list of all servers created and not deleted in the tenancy'''
      serversList = []
      servers_client = self.client.servers.list(detailed=False)
      
      #Remove from list servers deleted from the site
      aux_servers = self.servers[self.tenancyName].copy()
      for server in aux_servers:
         if not server in servers_client:
            self.servers[self.tenancyName].pop(server,None)
      
      #Add servers      
      for server in servers_client:
         serv = self.servers[self.tenancyName]
      
         if not server in serv or (server in serv and serv[server] != None and 'vcycle' in serv[server].name):
            serv[server] = self.client.servers.describe(server)
         serversList.append(serv[server])
      
      return serversList
   
         
   def _retrieve_properties(self, server, vmtypeName):
      '''Returns the server's properties'''
      properties = {}
      properties['startTime'] = int(server.created)
              
      try:
         properties['heartbeatTime'] = int(os.stat('/var/lib/vcycle/machines/' + server.name + '/machineoutputs/vm-heartbeat').st_ctime)
         properties['heartbeatStr'] = str(int(time.time() - properties['heartbeatTime'])) + 's'
      except:
         properties['heartbeatTime'] = None
         properties['heartbeatStr'] = '-'
      
      if len(server.ip) > 0:
         VCYCLE.logLine(server.name + ' ' +
                    (vmtypeName + ' ')[:16] +
                    (server.ip[0] + ' ')[:16] +
                    (server.status + ' ')[:8]
                    )
      else:
         VCYCLE.logLine(server.name + ' ' +
                    (vmtypeName + ' ')[:16] +
                    ('0.0.0.0' + ' ')[:16] +
                    (server.status + ' ')[:8]
                    )
      return properties
   
   
   def _update_properties(self, server, vmtypeName, runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
      '''Updates the server's properties'''
      if server.status in ['inactive','error','stopped']:
         VCYCLE.logLine(server.name + ' was a fizzle!' + str(int(time.time()) - properties['startTime']) + ' seconds')
      
      if server.status == 'active':
         # These ones are running properly
         totalRunning += 1

      if vmtypeName not in runningPerVmtype:
         runningPerVmtype[vmtypeName] = 1
      else:
         runningPerVmtype[vmtypeName] += 1

      # These ones are starting/running
      if server.status == 'active' and (int(time.time()) - properties['startTime']) < self.tenancy['vmtypes'][vmtypeName]['fizzle_seconds']:
         if vmtypeName not in notPassedFizzleSeconds:
            notPassedFizzleSeconds[vmtypeName] = 1
         else:
            notPassedFizzleSeconds[vmtypeName] += 1
      
      return totalRunning
   
   
   def _describe(self, server):
      '''Returns the descripion of a server'''
      return self.client.servers.describe(server)
   
   
   def _delete(self, server, vmtypeName, properties):
      '''Deletes a server'''
      if server.state == 'building':
         return False
      
      
      if server.status in ['inactive','error','stopped','cancel'] or self._condition_walltime(server, vmtypeName, properties) :
         if self._condition_walltime(server, vmtypeName, properties):
            VCYCLE.logLine("%s Walltime!!: %s > %s" % 
                           (server.name, (int(time.time()) - properties['startTime']),
                           self.tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds'])  )
         VCYCLE.logLine('Deleting ' + server.name)
         try:
            server.delete()
            return True
         except Exception as e:
            VCYCLE.logLine('Delete ' + server.name + ' fails with ' + str(e))
      return False
      
      
   def _server_name(self, name=None):
      '''Returns the server name'''
      if not name is None:
         return 'vcycle-' + name + '-' + str(int(time.time()))
      else:
         return 'vcycle-' + str(int(time.time()))
      
      
   def _create_machine(self, serverName, vmtypeName, proxy=False):
      '''Creates a new VM using OCCI interface'''
      tenancyName = self.tenancyName
      server = self.client.servers.create(serverName,
                VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['image_name'],
                VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['flavor_name'],
                user_data="file:///var/lib/vcycle/user_data/%s:%s" % (tenancyName, vmtypeName) )
      if 'network' in VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]:
         server.link(VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['network'])
      return server
   
   
   def _condition_walltime(self, server, vmtypeName, properties):
      return (server.status == 'active' and
        ((int(time.time()) - properties['startTime']) > self.tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds']))
      