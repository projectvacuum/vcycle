import os
import ConfigParser
import time # calls should be overridden in test patching
from mock import patch
import numpy as np
import pickle


from vcycle.core import vacutils
from vcycle.core import shared
from vcycle.core.shared import BaseSpace
from vcycle.core.shared import Machine
from vcycle.core.shared import MachineState
from vcycle.core.shared import VcycleError

class CycleTime(object):

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
  """ Test machine class that overrides functions we don't want """

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
  """ Class that keeps track of machine creation and deletion without actually
  doing anything
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

    # numpy array to keep track
    self.data = np.empty((cycles, len(self.machinetypeQueue)))
    self.cycles = cycles

  def run(self):
    for i in range(self.cycles):
      self.data[i] = self._countMachinetypes().values()
      self.cycle()
    print "\nsaving data"
    self.saveData()

  def saveData(self):
    # save data
    np.save(self.conf_path, self.data)

    # metadata
    machinetypes = []
    processors_limit = 0
    for space in self.spaces.values():
      machinetypes += space.machinetypes.keys()
      processors_limit += space.processors_limit
    metadata = {
        'machinetypes': machinetypes,
        'processors_limit': processors_limit
    }
    with open(self.conf_path + '.pkl', 'wb') as f:
      pickle.dump(metadata, f, pickle.HIGHEST_PROTOCOL)

  def setupQueues(self):
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
    """ Get's a job from a queue name
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
    print "cycle: {}/{}\n".format(self.ct.time(), self.cycles)
    print "queue: {}\n".format(self.queues)

    self.assignJobs()
    with patch('time.time', side_effect = self.ct.time) as mock_time:
      for space in self.spaces.values():
        space.oneCycle()
      self.ct.update()

  def _countMachinetypes(self):
    machineCount = {}
    for space in self.spaces.values():
      for mtn in space.machinetypes:
        machineCount[mtn] = 0
      for machine in space.machines.values():
        machineCount[machine.machinetypeName] += 1
    return machineCount
