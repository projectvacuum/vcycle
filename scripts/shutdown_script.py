#!/usr/bin/python

import os
import sys
import time
import argparse

import vcycle.shared
import vcycle.vacutils

""" Script to set shutdowntime for all active jobs
    Takes desired shutdown time as argument
"""

# parser settings
parser = argparse.ArgumentParser(description='Set shutdown time for jobs.')

parser.add_argument('shutdowntime', type=int,
    help='Shutdown time. Default in seconds since epoch.')

parser.add_argument('-s', '--spaces', type=str, nargs='*', default=[],
    help='Specify spaces to shutdown. If none it go through all spaces.')

parser.add_argument('-i', '--inseconds', action='store_true',
    help='Set shutdown time to be in n seconds.')

args = parser.parse_args(sys.argv[1:])

# set shutdown time depending on if inseconds was set
if args.inseconds:
  shutdowntime = int(time.time()) + args.shutdowntime
else:
  shutdowntime = args.shutdowntime

# check time is in future
if shutdowntime < time.time():
  print "Shutdown time must be in future."
  sys.exit(0)

# read configuration to get spaces
vcycle.shared.readConf()

for spaceName, space in vcycle.shared.spaces.iteritems():

  # skip spaces if not specified
  if args.spaces != [] and spaceName not in args.spaces:
      continue

  try:
    space.connect()
  except:
    print 'Could not connect to ', spaceName
    continue

  try:
    space.scanMachines()
  except:
    print 'Failed to scan machines for ', spaceName
    continue

  # iterate over machines
  for machineName, machine in space.machines.iteritems():

    jobfeaturespath = ('/var/lib/vcycle/machines/' + machineName
        + '/jobfeatures')
    try:
      shutdown_file = open(jobfeaturespath +'/shutdowntime_job', 'r')
    except:
      print 'Unable to open shutdowntime_job file'
      shutdowntime_job = None
    else:
      # if we were able to open it, try and read it
      try:
        shutdowntime_job = int(shutdown_file.read().strip())
        print time.asctime(time.localtime(shutdowntime_job)), \
            shutdowntime_job
      except:
        print 'unable to read shutdowntime_job file'

    # want to be able to write to it if shutdowntime_job doesn't exist or
    # shutdowntime is less
    if shutdowntime_job > shutdowntime or shutdowntime_job == None:
      print 'Updating shutdown time for job'
      shutdown_file = open(jobfeaturespath +'/shutdowntime_job', 'w')
      shutdown_file.write(str(shutdowntime))

    shutdown_file.close()

