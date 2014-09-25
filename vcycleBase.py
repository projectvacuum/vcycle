import VCYCLE
import os
import time, random
import abc

class vcycleBase(object):
   '''Base Class where other class inherit'''
   __metaclass__ = abc.ABCMeta
   
   creationsPerCycle = 5
   
   def __init__(self):
      pass
   
   def oneCycle(self, tenancyName, tenancy, servers):
      '''Principal method.
      Checks every vm running on all tenants. 
      If the vm is stopped or it was running more than 
      a period of time the vm will be deleted. 
      If there are free space, the method will create new vms.'''
  
      VCYCLE.logLine('Processing tenancy ' + tenancyName)
  
      totalRunning = 0
      totalFound   = 0

      notPassedFizzleSeconds = {}
      foundPerVmtype         = {}
      runningPerVmtype       = {}

      for vmtypeName,vmtype in tenancy['vmtypes'].iteritems(): 
         notPassedFizzleSeconds[vmtypeName] = 0
         foundPerVmtype[vmtypeName]         = 0
         runningPerVmtype[vmtypeName]       = 0
      
      self.tenancyName = tenancyName
      self.servers = servers
      self.tenancy = tenancy 
      self.client = self._create_client()
      
      #Update the servers running on the site   
      try:
         self._servers_list()
      except Exception as e:
         VCYCLE.logLine('novaClient.servers.list() fails with exception ' + str(e))
         return
      
      #Get the running and total found servers inside tenancy
      servers_in_tenancy = servers[tenancyName].copy()
      for oneServer in servers_in_tenancy:
         (totalRunning, totalFound) = self.for_server_in_list(servers_in_tenancy[oneServer], totalRunning, totalFound, notPassedFizzleSeconds, foundPerVmtype, runningPerVmtype)
            
      VCYCLE.logLine('Tenancy ' + tenancyName + ' has %d ACTIVE:running vcycle VMs out of %d found in any state for any vmtype or none' % (totalRunning, totalFound))
      for vmtypeName,vmtype in tenancy['vmtypes'].iteritems():
         VCYCLE.logLine('vmtype ' + vmtypeName + ' has %d ACTIVE:running out of %d found in any state' % (runningPerVmtype[vmtypeName], foundPerVmtype[vmtypeName]))
      
      # Now decide whether to create new VMs
      createdThisCycle = 0

      # Keep going till limits exhausted
      while True:
         createdThisPass = 0
         # Go through vmtypes, possibly creating one from each, before starting again
         vmtypeNames = tenancy['vmtypes'].keys()
         random.shuffle(vmtypeNames)
    
         for vmtypeName in vmtypeNames:
            vmtype = tenancy['vmtypes'][vmtypeName]
    
            if totalFound >= tenancy['max_machines']:
               VCYCLE.logLine('Reached limit (%d) on number of machines to create for tenancy %s' % (tenancy['max_machines'], tenancyName))
               return

            elif foundPerVmtype[vmtypeName] >= vmtype['max_machines']:
               VCYCLE.logLine('Reached limit (%d) on number of machines to create for vmtype %s' % (vmtype['max_machines'], vmtypeName))

            elif createdThisCycle >= self.creationsPerCycle:
               VCYCLE.logLine('Free capacity found ... but already created %d this cycle' % createdThisCycle )
               return

            elif int(time.time()) < (VCYCLE.lastFizzles[tenancyName][vmtypeName] + vmtype['backoff_seconds']):
               VCYCLE.logLine('Free capacity found for %s ... but only %d seconds after last fizzle' % (vmtypeName, int(time.time()) - VCYCLE.lastFizzles[tenancyName][vmtypeName]) )
        
            elif (int(time.time()) < (VCYCLE.lastFizzles[tenancyName][vmtypeName] + vmtype['backoff_seconds'] + vmtype['fizzle_seconds'])) and (notPassedFizzleSeconds[vmtypeName] > 0):
                VCYCLE.logLine('Free capacity found for %s ... but still within fizzleSeconds+backoffSeconds(%d) of last fizzle (%ds ago) and %d running but not yet passed fizzleSeconds (%d)' % 
                (vmtypeName, vmtype['fizzle_seconds'] + vmtype['backoff_seconds'], int(time.time()) - VCYCLE.lastFizzles[tenancyName][vmtypeName], notPassedFizzleSeconds[vmtypeName], vmtype['fizzle_seconds']))

            else:
               VCYCLE.logLine('Free capacity found for ' + vmtypeName + ' within ' + tenancyName + ' ... creating')
               errorMessage = self.createMachine(vmtypeName, servers, proxy='proxy' in tenancy)
               if errorMessage:
                  VCYCLE.logLine(errorMessage)
               else:
                  createdThisCycle                   += 1
                  createdThisPass                    += 1
                  totalFound                         += 1
                  foundPerVmtype[vmtypeName]         += 1
                  notPassedFizzleSeconds[vmtypeName] += 1
              
         if createdThisPass == 0:
            # Run out of things to do, so finish the cycle for this tenancy
            return
         
   
   def for_server_in_list(self, server, totalRunning, totalFound,
                          notPassedFizzleSeconds, foundPerVmtype, runningPerVmtype):
      '''Executes for every server found in the tenancy, if the server is stopped or it has been running
      more than an specific time, the method will delete the server.'''
      
      servers = self.servers[self.tenancyName]
      # This includes VMs that we didn't create and won't manage, to avoid going above tenancy limit
      totalFound += 1
      
      # Just in case other VMs are in this tenancy
      if server is None or server.name[:7] != 'vcycle-':
        return (totalRunning , totalFound)
     
      try:
         fileTenancyName = open('/var/lib/vcycle/machines/' + server.name + '/tenancy_name', 'r').read().strip()
      except:
         # Not one of ours? Cleaned up directory too early?
         VCYCLE.logLine('Skipping ' + server.name + ' which has no tenancy name')
         return (totalRunning , totalFound)
      else:
         # Weird inconsistency, maybe the name changed? So log a warning and ignore this VM
         if fileTenancyName != self.tenancyName:        
            VCYCLE.logLine('Skipping ' + server.name + ' which is in ' + self.tenancy['tenancy_name'] + ' but has tenancy_name=' + fileTenancyName)
            return (totalRunning , totalFound)

      try:
         vmtypeName = open('/var/lib/vcycle/machines/' + server.name + '/vmtype_name', 'r').read().strip()
      except:
         # Not one of ours? Something went wrong?
         VCYCLE.logLine('Skipping ' + server.name + ' which has no vmtype name')
         return (totalRunning , totalFound)

      if vmtypeName not in foundPerVmtype:
        foundPerVmtype[vmtypeName]  = 1
      else:
        foundPerVmtype[vmtypeName] += 1
        
      properties = self._retrieve_properties(server, vmtypeName)
      totalRunning = self._update_properties(server, vmtypeName, runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning)
      self._delete(server, vmtypeName, properties)
      servers.pop(server.id, None)
      return (totalRunning , totalFound)


   def createMachine(self, vmtypeName, servers, proxy=False):
      '''Creates a new VM'''
      
      tenancyName = self.tenancyName
      serverName = self._server_name(name=tenancyName)
      os.makedirs('/var/lib/vcycle/machines/' + serverName + '/machinefeatures')
      os.makedirs('/var/lib/vcycle/machines/' + serverName + '/jobfeatures')
      os.makedirs('/var/lib/vcycle/machines/' + serverName + '/machineoutputs')

      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/vmtype_name',  vmtypeName,  0644)
      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/tenancy_name', tenancyName, 0644)

      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures/phys_cores', '1',        0644)
      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures/vac_vmtype', vmtypeName, 0644)
      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures/vac_space',  VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['ce_name'],0644)

      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/jobfeatures/cpu_limit_secs',  str(VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['max_wallclock_seconds']), 0644)
      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/jobfeatures/wall_limit_secs', str(VCYCLE.tenancies[tenancyName]['vmtypes'][vmtypeName]['max_wallclock_seconds']), 0644)

      try:
         server = self._create_machine(serverName, vmtypeName, proxy=proxy)
         if not server is None:
            servers[self.tenancyName][server.id] = server
      except Exception as e:
         return 'Error creating new server: ' + str(e)
      
      if not server is None:
         VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures/vac_uuid', server.id, 0644)
      VCYCLE.makeJsonFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures')
      VCYCLE.makeJsonFile('/var/lib/vcycle/machines/' + serverName + '/jobfeatures')

      if not server is None:
         VCYCLE.logLine('Created ' + serverName + ' (' + server.id + ') for ' + vmtypeName + ' within ' + tenancyName)
      else:
         VCYCLE.logLine('Created ' + serverName + ' for ' + vmtypeName + ' within ' + tenancyName)
      return None


   @abc.abstractmethod
   def _create_client(self):
      '''Creates a new Client. It is an abstract method'''
      pass


   @abc.abstractmethod
   def _servers_list(self):
      '''Returns a list with of the servers created in a tenancy. It is an abstract method'''
      pass
   
   
   @abc.abstractmethod
   def _retrieve_properties(self, server, vmtypeName):
      '''Returns the properties of a VM. It is an abstract method'''
      pass

   
   @abc.abstractmethod
   def _update_properties(self, server, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
      '''Updates the properties of a VM'''
      pass
   
   
   @abc.abstractmethod
   def _describe(self, server):
      '''Returns the description of a VM.'''
      pass
   
   @abc.abstractmethod
   def _delete(self, server, vmtypeName, properties):
      '''Deletes a VM'''
      pass
   
   
   @abc.abstractmethod
   def _server_name(self,name=None):
      '''Returns the name of a VM'''
      pass


   @abc.abstractmethod
   def _create_machine(self, serverName, vmtypeName, proxy=False):
      '''Creates a new VM inside a tenancy'''
      pass
