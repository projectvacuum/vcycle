import unittest
import mock

from vcycle.core import vacutils
from vcycle.core.shared import BaseSpace
from vcycle.core.shared import Machine
from vcycle.core.shared import MachineState
from vcycle.core.shared import VcycleError

class TestMachine():
  """ Test machine class that that doesn't do any file creation """

  def __init__(
      self, name, spaceName, state, ip, createdTime, startedTime, updatedTime,
      uuidStr, machinetypeName, zone = None, processors = None):
    self.name            = name
    self.spaceName       = spaceName
    self.state           = state
    self.ip              = ip
    self.createdTime     = createdTime
    self.startedTime     = startedTime
    self.updatedTime     = updatedTime
    self.uuidStr         = uuidStr
    self.machinetypeName = machinetypeName
    self.zone            = zone
    self.processors      = processors
    self.hs06            = None
    self.managedHere     = True
    self.deletedTime     = None
    self.stoppedTime     = None

    self.cycleNum = 0 # cycle number to be incremented

  def getFileContents(self, fileName):
    pass

  def setFileContents(self, fileName, contents):
    pass

  def writeApel(self):
    pass

  def sendMachineMessage(self, cookie):
    pass

  def setShutdownMessage(self):
    pass

  def setHeartbeatTime(self):
    pass

  def update(self):
    vacutils.logLine('Updating machine')
    self.cycleNum += 1

class TestSpace(BaseSpace):
  """ Class that keeps track of machine creation and deletion without actually
  doing anything
  """

  def __init__(
      self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
    super(TestSpace, self).__init__(
        api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    # additional variables that the test space uses
    self.cycleNum = 0
    self.machinesToDelete = []

  def oneCycle(self):
    """ Override standard oneCycle

    Don't care about connect, sendVacMon, updateGOCDB or
    createHeartbeatMachines, and we'd like to have update method that changes
    state of machines
    """

    try:
      self.updateMachines()
    except Exception as e:
      raise VcycleError('Updating machines in ' + self.spaceName
          + ' fails: ' + str(e))

    try:
      self.deleteMachines()
      for i in self.machinesToDelete:
        del self.machines[i]
    except Exception as e:
      vacutils.logLine('Deleting machines in ' + self.spaceName
          + ' fails: ' + str(e))

    try:
      self.makeMachines()
    except Exception as e:
      vacutils.logLine('Making machines in ' + self.spaceName
          + ' fails: ' + str(e))

  def scanMachines(self):
    """ Null as we aren't starting up the class every time """
    pass

  def getFlavorName(self):
    pass

  def getImageID(self):
    pass

  def uploadImage(self):
    pass

  def getKeyPairName(self):
    pass

  def createMachine(self, machineName, machinetypeName, zone = None):
    """ Used to generate test machines """
    # create machine
    self.machines[machineName] = TestMachine(
        name            = machineName,
        spaceName       = self.spaceName,
        state           = MachineState.starting,
        ip              = None,
        createdTime     = 0,
        startedTime     = 0,
        updatedTime     = 0,
        uuidStr         = None,
        machinetypeName = machinetypeName,
        zone            = zone,
        processors      = 1)

    # update space totals
    machine = self.machines[machineName]
    self.totalMachines += 1
    self.totalProcessors += machine.processors
    self.runningMachines += 1
    self.runningProcessors += machine.processors

    machinetype = self.machinetypes[machinetypeName]
    machinetype.totalMachines += 1
    machinetype.totalProcessors += machine.processors
    # for now assume it's running from the beginning
    machinetype.runningMachines += 1
    machinetype.runningProcessors += machine.processors

  def deleteOneMachine(self, machineName):
    """ Note machine to delete and update totals """

    self.machinesToDelete.append(machineName)

    # update totals
    machine = self.machines[machineName]
    self.totalMachines -= 1
    self.totalProcessors -=  machine.processors
    self.runningMachines -= 1
    self.runningProcessors -=  machine.processors

    # update machinetype totals
    machinetype = self.machinetypes[machine.machinetypeName]
    machinetype.totalMachines -= 1
    machinetype.totalProcessors -= machine.processors
    machinetype.runningMachines -= 1
    machinetype.runningProcessors -= machine.processors

  def updateMachines(self):
    """ Simulate vm's running """

    vacutils.logLine('Updating machines in ' + self.spaceName)

    self.cycleNum += 1

    # update individual machines
    for _, machine in self.machines.iteritems():
      machine.update()
