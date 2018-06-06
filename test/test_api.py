import unittest
import mock

from vcycle.core import vacutils
from vcycle.core.shared import BaseSpace
from vcycle.core.shared import Machine
from vcycle.core.shared import MachineState
from vcycle.core.shared import VcycleError

class CycleTime():

  def __init__(self):
    self.cycle = 0

  def time(self):
    return self.cycle

  def update(self):
    self.cycle += 1

class TestMachine():
  """ Test machine class that overrides functions we don't want """

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

    # function that changes depending on state
    self._update = self._startUpdate
    self.timeInState = 0

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
    self.timeInState += 1
    self._update()

  def _startUpdate(self):
    self._update = self._runningUpdate
    self.state = MachineState.running
    self.timeInState = 0

  def _runningUpdate(self):
    if self.timeInState > 10:
      self._update = self._stoppingUpdate
      self.state = MachineState.stopping
      self.timeInState = 0

  def _stoppingUpdate(self):
    self.state = MachineState.shutdown
    self.timeInState = 0

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

    self.updateMachines()
    self.updateTotals()
    self.deleteMachines()
    self.cleanMachines()
    self.makeMachines()

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
    machine = TestMachine(
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

    self.machines[machineName] = machine

  def deleteOneMachine(self, machineName):
    """ Note machine to delete """

    self.machinesToDelete.append(machineName)

  def updateMachines(self):
    """ Simulate vm's running """

    # update individual machines
    for machine in self.machines.values():
      machine.update()

  def cleanMachines(self):
    """ Delete machines based of machinesToDelete """
    for machine in self.machinesToDelete:
      del self.machines[machine]

    self.machinesToDelete = []

  def updateTotals(self):

    # reset numbers
    self.totalMachines     = 0
    self.totalProcessors   = 0
    self.runningMachines   = 0
    self.runningProcessors = 0

    for mt in self.machinetypes.values():
      mt.totalMachines      = 0
      mt.totalProcessors    = 0
      mt.runningMachines    = 0
      mt.runningProcessors  = 0
      mt.startingProcessors = 0

    self.updateMachineTotals()
