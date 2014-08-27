from vcycleBase import vcycleBase
import os
import VCYCLE
import time
from occi import Occi

class vcycleOcci(vcycleBase):
   
   
   def _create_client(self, tenancy):
      if 'proxy' in tenancy:
          return Occi(tenancy['url'],user_cred=tenancy['proxy'])
      else:
         return Occi(tenancy['url'],
                                username=tenancy['username'],
                                password=tenancy['password'],
                                auth=tenancy['auth'],
                                voms=False)
   
   
   def _retrieve_properties(self, server, vmtypeName):
      properties = {}
      properties['startTime'] = int(server.created)
              
      try:
         properties['heartbeatTime'] = int(os.stat('/var/lib/vcycle/machines/' + server.name + '/machineoutputs/vm-heartbeat').st_ctime)
         properties['heartbeatStr'] = str(int(time.time() - properties['heartbeatTime'])) + 's'
      except:
         properties['heartbeatTime'] = None
         properties['heartbeatStr'] = '-'

      VCYCLE.logLine(server.name + ' ' +
                    (vmtypeName + ' ')[:16] +
                    (server.ip + ' ')[:16] +
                    (server.status + ' ')[:8]
                    )
      return properties
   
   
   def _update_properties(self, server, tenancy, tenancyName, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
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
      if server.status == 'active' and (int(time.time()) - properties['startTime']) < tenancy['vmtypes'][vmtypeName]['fizzle_seconds']:
         if vmtypeName not in notPassedFizzleSeconds:
            notPassedFizzleSeconds[vmtypeName] = 1
         else:
            notPassedFizzleSeconds[vmtypeName] += 1
      
      return totalRunning
   
   
   def _delete(self, server, tenancy, vmtypeName, properties):
      if server.status in ['inactive','error','stopped','cancel'] or (server.status == 'active' and
        ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds'])) :
         VCYCLE.logLine('Deleting ' + server.name)
         try:
            server.delete()
         except Exception as e:
            VCYCLE.logLine('Delete ' + server.name + ' fails with ' + str(e))
      
      
   def _server_name(self,name=None):
      if not name is None:
         return 'vcycle-' + name + '-' + str(int(time.time()))
      else:
         return 'vcycle-' + str(int(time.time()))
      
      
   def _create_machine(self, client, serverName, tenancyName, vmtypeName, proxy=False):
      return client.servers.create(serverName,
                VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['image_name'],
                VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['flavor_name'],
                user_data="file:///var/lib/vcycle/user_data/%s:%s" % (tenancyName, vmtypeName) )