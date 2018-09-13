import os
import ConfigParser
import time # calls should be overridden in test patching
from mock import patch
import numpy as np


from vcycle.core import vacutils
from vcycle.core import shared
from vcycle.core.shared import BaseSpace
from vcycle.core.shared import Machine
from vcycle.core.shared import MachineState
from vcycle.core.shared import VcycleError


class CycleTime(object):
  """Cycle time class

  Can be used to keep track of cycle count, patch time.time() with the
  time method to run vcycle in terms of cycles.
  """

  def __init__(self):
    self.cycle = 1 # start at one else bool statements fudge up :(

  def time(self):
    return self.cycle

  def update(self):
    self.cycle += 1


class JobState:
  starting = 'Starting'
  requesting = 'Requesting Job'
  noJob = 'No Jobs available'
  running = 'Job running'
  finished = 'Job executed'
  failed = 'Job failed'


class TestMachine(Machine):
  """Test machine class

  Stubs out various function and simulates a running machine via update
  functions.
  """

  def __init__(
      self, name, spaceName, state, ip, createdTime, startedTime, updatedTime,
      uuidStr, machinetypeName, zone = None, processors = None):
    super(TestMachine, self).__init__(
      name, spaceName, state, ip, createdTime, startedTime, updatedTime,
      uuidStr, machinetypeName, zone = None, processors = None)

    # function that changes depending on state
    self._update = self._startUpdate
    self.timeInState = 0
    self.timeToStop = 2 + 0.1 * np.random.randn()

    self.job = JobState.starting
    self.jobID = None

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
    if self.timeInState < self.timeToStop:
      return

    self._update = self._runningUpdate
    self.state = MachineState.running
    self.timeInState = 0
    self.timeToStop = 100 + 20 * np.random.randn()
    self.job = JobState.requesting

  def _runningUpdate(self):

    # if no job is found
    if self.job == JobState.noJob:
      self._update = self._stoppingUpdate
      self.state = MachineState.stopping
      self.timeInState = 0
      return

    # shutdown after a while
    if self.timeInState < self.timeToStop:
      return
    self._update = self._stoppingUpdate
    self.state = MachineState.stopping
    self.timeInState = 0

  def _stoppingUpdate(self):
    self.state = MachineState.shutdown
    self.timeInState = 0

  def shutdownSignal(self):
    # set timeToStop to be closer
    shutdownTime = self.timeInState + np.random.random() * 15
    self.timeToStop = shutdownTime if shutdownTime < self.timeToStop else self.timeToStop
    self.state = MachineState.stopping


class TestSpace(BaseSpace):
  """Class that keeps track of test machines

  Inherits base space but overrides and stubs various functionality
  Should be used in context where time is patched to return cycle count
  """

  def __init__(
      self, api, apiVersion, spaceName, parser, spaceSectionName, updatePipes):
    super(TestSpace, self).__init__(
        api, apiVersion, spaceName, parser, spaceSectionName, updatePipes)

    # additional variables that the test space uses
    self.machinesToDelete = []

  def oneCycle(self):
    """ Override standard oneCycle

    Don't care about connect, sendVacMon, updateGOCDB or
    createHeartbeatMachines, and we'd like to have update method that
    changes state of machines
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
        createdTime     = int(time.time()),
        startedTime     = int(time.time()),
        updatedTime     = int(time.time()),
        uuidStr         = None,
        machinetypeName = machinetypeName,
        zone            = zone,
        processors      = 1)

    machine.managedHere = True
    machine.deletedTime = None

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
      mt.notPassedFizzle    = 0
      mt.weightedMachines   = 0

    for machineName in self.machines:
      self._countMachine(machineName)

  def shutdownOneMachine(self, machineName):
    self.machines[machineName].shutdownSignal()


class TestManager(object):
  """ Manages test space objects and their queues """

  def __init__(self, configFile, cycles):
    # initialise parser
    parser = ConfigParser.RawConfigParser()
    self.conf_path = (os.path.abspath(os.path.dirname(__file__))
        + '/test_configs/' + configFile)
    parser.read(self.conf_path)
    self.parser = parser

    self.queues = {}
    self._queueIDs = {}
    self.machinetypeQueue = {}
    self.setupQueues()

    shared.readConf(parser = parser, updatePipes = False)
    self.spaces = shared.getSpaces()
    self.ct = CycleTime()

    machinetypes = []
    self.processors_limit = 0
    for space in self.spaces.values():
      machinetypes += space.machinetypes
      self.processors_limit += space.processors_limit

    # numpy array to keep track
    self.mcdtype = np.dtype([
        (i, [
          ('total', 'int_'), ('starting', 'int_'),
          ('running', 'int_'), ('stopping', 'int_')])
        for i in machinetypes])
    self.data = np.empty(cycles, dtype=self.mcdtype)
    self.cycles = cycles

  def run(self):
    """Method to run test simulation"""
    for i in range(self.cycles):
      self.data[i] = self._countMachinetypes()
      self.cycle()
    print "\nsaving data"
    self.save()

  def save(self):
    """Save meta and simulation data"""
    metadata = {'processors_limit': self.processors_limit}

    data = np.array({'metadata': metadata, 'data': self.data})

    np.save(self.conf_path, data)

  def setupQueues(self):
    """Sets up queues using config files

    Creates dictionary of queues, mapping of machinetypes to queues.
    Removes these sections and options from parser as not cause issues
    with the rest of vcycle.
    """

    for sec in self.parser.sections():
      (secType, secName) = sec.lower().split(None,1)
      if secType == 'queue':
        events = eval(self.parser.get(sec, 'events'))
        self.queues[secName] = events
        self._queueIDs[secName] = 0
        self.parser.remove_section(sec) # clean up section

      try:
        (secType, spaceType, mtName) = sec.lower().split(None, 2)
      except ValueError:
        continue
      if secType == 'machinetype':
        qName = self.parser.get(sec, 'queue')
        self.machinetypeQueue[mtName] = qName
        self.parser.remove_option(sec, 'queue')

  def assignJobs(self):
    """Assigns jobs from queues to machines"""

    for space in self.spaces.values():
      for machine in space.machines.values():
        if machine.job == JobState.requesting:
          jobID = self.getJob(machine)
          if jobID:
            machine.job = JobState.running
            machine.jobID = jobID
          else:
            machine.job = JobState.noJob
            mtName = machine.machinetypeName
            space.machinetypes[mtName].lastAbortTime = self.ct.time()

  def getJob(self, machine):
    """ Get's a job for a machine
    returns id if a job is available
    returns None if not
    """

    queueName = self.machinetypeQueue[machine.machinetypeName]
    q = self.queues[queueName]

    if not q:
      return None
    (t, jobs) = q[0]
    if t >= self.ct.time():
      return None

    # get jobID and increment
    jobID = self._queueIDs[queueName]
    self._queueIDs[queueName] += 1

    if (jobs - 1) <= 0:
      q.pop(0)
    else:
      q[0] = (t, jobs - 1)

    return queueName + '-' + str(jobID)

  def cycle(self):
    """Assigns jobs, cycles all spaces and updates cycle time"""

    print "cycle: {}/{}".format(self.ct.time(), self.cycles)
    print "queue: {}".format(self.queues)

    self.assignJobs()
    with patch('time.time', side_effect = self.ct.time) as mock_time:
      for space in self.spaces.values():
        space.oneCycle()
      self.ct.update()

  def _countMachinetypes(self):
    """Count up number of each machine type"""

    machineCount = np.empty(1, dtype=self.mcdtype)[0] # TODO probably a neater way of creating that dtype element
    for space in self.spaces.values():

      for mtn in space.machinetypes:
        machineCount[mtn] = (0, 0, 0, 0)

      for machine in space.machines.values():
        mCount = machineCount[machine.machinetypeName]
        mCount['total'] += 1
        if machine.state == MachineState.starting:
          mCount['starting'] += 1
        elif machine.state == MachineState.running:
          mCount['running'] += 1
        elif machine.state == MachineState.stopping:
          mCount['stopping'] += 1

    print machineCount
    return machineCount
