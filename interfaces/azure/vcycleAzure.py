from vcycleBase import vcycleBase
import os
import VCYCLE
import uuid
import time , calendar
import interfaces.azure.client


class vcycleAzure(vcycleBase):
   
   def _create_client(self):
      '''Creates a new Azure client'''
      tenancy = self.tenancy
      self.provider_name = tenancy['tenancy_name']
      return interfaces.azure.client.Azure(tenancy['proxy'], tenancy['tenancy_name'])
   
   
   def _servers_list(self):
      '''Returns a list of all servers created and not deleted in the tenancy'''
      return self.client.list_vms()
      
   
   def _retrieve_properties(self, server, vmtypeName, servers):
      '''Returns the server's properties'''
      properties = {}
      start_time = server.name[server.name.find("-",server.name.find('-')+1)+1:]
      properties['startTime'] = int(start_time)
            
      properties['ip'] = server.ip
                 
      try:
        properties['heartbeatTime'] = int(os.stat('/var/lib/vcycle/machines/' + server['name'] + '/machineoutputs/vm-heartbeat').st_ctime)
        properties['heartbeatStr'] = str(int(time.time() - properties['heartbeatTime'])) + 's'
      except:
        properties['heartbeatTime'] = None
        properties['heartbeatStr'] = '-'
        
      try:
         properties['fizzleTime'] = int(os.stat('/var/lib/vcycle/machines/' + server.name + '/machineoutputs/vm-start').st_ctime)
         properties['fizzleStr'] = str(int(properties['fizzleTime']) - int(properties['startTime'])) + 's'
         servers[server.name]['fizzle'] = int(properties['startTime']) - int(servers[server.name]['start_time'])
      except Exception:
         properties['fizzleTime'] = None
         properties['fizzleStr'] = '-'
      
      VCYCLE.logLine(self.tenancyName, server.name + ' ' + 
              (vmtypeName + '                  ')[:16] + 
              (properties['ip'] + '            ')[:16] + 
              (server.state + '       ')[:8] + 
              properties['fizzleStr'] + " "  +
              properties['heartbeatStr'] + " " +
              str(int(time.time()) - properties['startTime'] ) + "s"
              )
      return properties
   
   
   def _update_properties(self, server, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
      '''Updates the server's properties'''
      tenancy = self.tenancy
      tenancyName = self.tenancyName
      
      if server.state == 'Stopped' and (properties['updatedTime'] - properties['startTime']) < tenancy['vmtypes'][vmtypeName]['fizzle_seconds']:
        VCYCLE.logLine(tenancyName, server.name + ' was a fizzle! ' + str(properties['updatedTime'] - properties['startTime']) + ' seconds')
        try:
          VCYCLE.lastFizzles[tenancyName][vmtypeName] = properties['updatedTime']
        except:
          # In case vmtype removed from configuration while VMs still existed
          pass

      if server.state == 'Started':
        # These ones are running properly
        totalRunning += 1
        
        if vmtypeName not in runningPerVmtype:
          runningPerVmtype[vmtypeName]  = 1
        else:
          runningPerVmtype[vmtypeName] += 1

      # These ones are starting/running, but not yet passed tenancy['vmtypes'][vmtypeName]['fizzle_seconds']
      if server.state in ['Starting','Started'] and \
          (int(time.time() - properties['startTime']) < tenancy['vmtypes'][vmtypeName]['fizzle_seconds']):
          
        if vmtypeName not in notPassedFizzleSeconds:
          notPassedFizzleSeconds[vmtypeName]  = 1
        else:
          notPassedFizzleSeconds[vmtypeName] += 1
      
      return totalRunning
      
      
   def _describe(self, server):
      '''Returns the descripion of a server. This method is empty because when the server is created,
      Azure returns directly all the vm description'''
      pass
      
      
   def _delete(self, server, vmtypeName, properties):
      '''Deletes a server'''
      tenancy = self.tenancy
      if server.state == 'Starting':
         return False
      
      if server.state == 'Stopped' or \
        (server.state == 'Started' and ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds'])) or \
        (
             # STARTED gets deleted if heartbeat defined in configuration but not updated by the VM
             'heartbeat_file' in tenancy['vmtypes'][vmtypeName] and
             'heartbeat_seconds' in tenancy['vmtypes'][vmtypeName] and
             server.state == 'Started' and 
             ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds']) and
             (
              (properties['heartbeatTime'] is None) or 
              ((int(time.time()) - properties['heartbeatTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds'])
             )              
           ):
          
         
        VCYCLE.logLine(self.tenancyName, 'Deleting ' + server.name)
        try:
          self.client.delete_vm(server.name)
          return True
        except Exception as e:
          VCYCLE.logLine(self.tenancyName, 'Delete ' + server.name + ' fails with ' + str(e))
      return False
          
          
   def _server_name(self, name=None):
      '''Returns the server name'''
      return 'vcycle-' + name + '-' + str(int(time.time()))
   
   
   def _create_machine(self, server_name, vmtypeName, proxy=False):
      import base64
      tenancy_name = self.tenancyName
      user_data = open("/var/lib/vcycle/user_data/%s:%s" % (tenancy_name, vmtypeName), 'r').read()
      return self.client.create_virtual_machine(server_name,
                                                username=VCYCLE.tenancies[tenancy_name]['vmtypes'][vmtypeName]['username'],
                                                password=VCYCLE.tenancies[tenancy_name]['vmtypes'][vmtypeName]['password'],
                                                image_name=VCYCLE.tenancies[tenancy_name]['vmtypes'][vmtypeName]['image_name'],
                                                flavor=VCYCLE.tenancies[tenancy_name]['vmtypes'][vmtypeName]['flavor_name'],
                                                user_data="/var/lib/vcycle/user_data/%s:%s" % (tenancy_name, vmtypeName))