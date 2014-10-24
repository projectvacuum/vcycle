from vcycleBase import vcycleBase
import os
import VCYCLE
import uuid
import time , calendar
import interfaces.dbce.client


class vcycleDBCE(vcycleBase):
   
   servers_contextualized = {}
   
   def _create_client(self):
      '''Create a new DBCE client'''
      tenancy = self.tenancy
      self.provider_name = tenancy['tenancy_name']
      dbceClient = interfaces.dbce.client.DBCE(tenancy['url'], tenancy['username'], tenancy['password'])
      return dbceClient
   
   
   def _servers_list(self):
      '''Returns a list of all servers created and not deleted in the tenancy'''
      serversList = self.client.machine.list(self.provider_name)
      for server in serversList:
         if not server.id in self.servers[self.tenancyName]:
            self.servers[self.tenancyName][server.id] = server
            if 'vcycle' in server.name:
               self.servers_contextualized[server.id] = {'server':server,'contextualized':False}
         if 'vcycle' in server.name and \
            self.servers_contextualized[server.id]['contextualized'] is False \
            and server.status == 'STARTED':
               self._contextualize(server)
      return serversList
   
   
   def _retrieve_properties(self, server, vmtypeName):
      '''Returns the server's properties'''
      properties = {}
      properties['createdTime']  = calendar.timegm(time.strptime(server.created, "%Y-%m-%dT%H:%M:%SZ"))
      properties['updatedTime']  = calendar.timegm(time.strptime(server.updated, "%Y-%m-%dT%H:%M:%SZ"))
      properties['startTime']    = properties['createdTime'] 
      
      properties['ip'] = '0.0.0.0'
      for address in server.network_interfaces:
         if 'PUBLIC' in address['id']:
            properties['ip'] = address['address']['ip']
           
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
        VCYCLE.logLine(tenancyName, server.name + ' was a fizzle! ' + str(properties['updatedTime'] - properties['startTime']) + ' seconds')
        try:
          VCYCLE.lastFizzles[tenancyName][vmtypeName] = properties['updatedTime']
        except:
          # In case vmtype removed from configuration while VMs still existed
          pass

      if server.status == 'STARTED':
        # These ones are running properly
        totalRunning += 1
        
        if vmtypeName not in runningPerVmtype:
          runningPerVmtype[vmtypeName]  = 1
        else:
          runningPerVmtype[vmtypeName] += 1

      # These ones are starting/running, but not yet passed tenancy['vmtypes'][vmtypeName]['fizzle_seconds']
      if ((server.status == 'STARTED' or 
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
      
      if server.status in ['SHUTOFF','ERROR','DELETED'] or \
        (server.status == 'STARTED' and ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['max_wallclock_seconds'])) or \
        (
             # STARTED gets deleted if heartbeat defined in configuration but not updated by the VM
             'heartbeat_file' in tenancy['vmtypes'][vmtypeName] and
             'heartbeat_seconds' in tenancy['vmtypes'][vmtypeName] and
             server.status == 'STARTED' and 
             ((int(time.time()) - properties['startTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds']) and
             (
              (properties['heartbeatTime'] is None) or 
              ((int(time.time()) - properties['heartbeatTime']) > tenancy['vmtypes'][vmtypeName]['heartbeat_seconds'])
             )              
           ):
          
         
        VCYCLE.logLine(self.tenancyName, 'Deleting ' + server.name)
        try:
          self.client.machine.delete(server.id)
          self.servers_contextualized.pop(server.id, None)
          return True
        except Exception as e:
          VCYCLE.logLine(self.tenancyName, 'Delete ' + server.name + ' fails with ' + str(e))
      return False
          
   def _server_name(self, name=None):
      '''Returns the server name'''
      return 'vcycle-' + str(uuid.uuid4())
   
   
   def _create_machine(self, serverName, vmtypeName, proxy=False):
      tenancyName = self.tenancyName
      template = self.client.machine_template.find(VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['image_name'],
                                                   int(VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['flavor_name']),
                                                   self.provider_name)[0]
   
      server = self.client.machine.create(serverName, serverName, template, self.provider_name, key_data=[VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['public_key']])
      self.servers_contextualized[server.id] = {'server':server,'contextualized':False}
      return server
   
   def _contextualize(self, server):
      for interface in server.network_interfaces:
         if 'PUBLIC' in interface['id']:
            try:
               import subprocess
               subprocess.check_call("scp -i /home/lvillazo/dbce/id_rsa -o StrictHostKeyChecking=no  /var/lib/vcycle/scripts/* dbce@%s:/home/dbce" % interface['address']['ip'],shell=True, stderr=subprocess.STDOUT)
            except Exception,e:
               pass
            else:
               subprocess.check_call("ssh -tt -i /home/lvillazo/dbce/id_rsa -o StrictHostKeyChecking=no dbce@%s 'chmod u+x build.sh;chmod u+x kvimg_context_script.sh;sudo nohup ./build.sh >> build.log 2>&1'&" % interface['address']['ip'], shell=True, stderr=subprocess.STDOUT)
               VCYCLE.logLine(self.tenancyName, 'Contextualized ' + server.name)
               self.servers_contextualized[server.id]['contextualized'] = True
            