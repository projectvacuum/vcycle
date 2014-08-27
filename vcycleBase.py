import VCYCLE
import os
import uuid
import time, random
import abc

class vcycleBase(object):
   __metaclass__ = abc.ABCMeta
   
   creationsPerCycle = 5
   
   def __init__(self):
      pass
   
   def oneCycle(self, tenancyName, tenancy):
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
         
      client = self._create_client(tenancy)
      
      try:
         serversList = client.servers.list(detailed=True)
      except Exception as e:
         VCYCLE.logLine('novaClient.servers.list() fails with exception ' + str(e))
         return
      
      for oneServer in serversList:
         (totalRunning, totalFound) = self.for_server_in_list(oneServer, tenancy, tenancyName, totalRunning, totalFound, notPassedFizzleSeconds, foundPerVmtype, runningPerVmtype)
      
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
               errorMessage = self.createMachine(client, tenancyName, vmtypeName,proxy='proxy' in tenancy)
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
         
   
   def for_server_in_list(self,server, tenancy, 
                          tenancyName, totalRunning, totalFound,
                          notPassedFizzleSeconds, foundPerVmtype, runningPerVmtype):
      # This includes VMs that we didn't create and won't manage, to avoid going above tenancy limit
      totalFound += 1
      
      # Just in case other VMs are in this tenancy
      if server.name[:7] != 'vcycle-':
        return (totalRunning , totalFound)
     
      try:
         fileTenancyName = open('/var/lib/vcycle/machines/' + server.name + '/tenancy_name', 'r').read().strip()
      except:
         # Not one of ours? Cleaned up directory too early?
         VCYCLE.logLine('Skipping ' + server.name + ' which has no tenancy name')
         return (totalRunning , totalFound)
      else:
         # Weird inconsistency, maybe the name changed? So log a warning and ignore this VM
         if fileTenancyName != tenancyName:        
            VCYCLE.logLine('Skipping ' + server.name + ' which is in ' + tenancy['tenancy_name'] + ' but has tenancy_name=' + fileTenancyName)
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
      totalRunning = self._update_properties(server, tenancy, tenancyName, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning)
      self._delete(server, tenancy, vmtypeName, properties)
      return (totalRunning , totalFound)


   def createMachine(self, client, tenancyName, vmtypeName, proxy=False):
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
         server = self._create_machine(client, serverName, tenancyName, vmtypeName, proxy=proxy)

      except Exception as e:
         return 'Error creating new server: ' + str(e)

      VCYCLE.createFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures/vac_uuid', server.id, 0644)
      VCYCLE.makeJsonFile('/var/lib/vcycle/machines/' + serverName + '/machinefeatures')
      VCYCLE.makeJsonFile('/var/lib/vcycle/machines/' + serverName + '/jobfeatures')

      VCYCLE.logLine('Created ' + serverName + ' (' + server.id + ') for ' + vmtypeName + ' within ' + tenancyName)

      return None


   @abc.abstractmethod
   def _create_client(self):
      pass

   
   @abc.abstractmethod
   def _retrieve_properties(self):
      pass

   
   @abc.abstractmethod
   def _update_properties(self, server, tenancy, tenancyName, vmtypeName,runningPerVmtype, notPassedFizzleSeconds, properties, totalRunning):
      pass
   
   
   @abc.abstractmethod
   def _delete(self, server):
      pass
   
   
   @abc.abstractmethod
   def _server_name(self,name=None):
      pass


   @abc.abstractmethod
   def _create_machine(self, client, serverName, tenancyName, vmtypeName, proxy=False):
      pass
