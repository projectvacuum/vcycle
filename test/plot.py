#! /usr/bin/python2

import os
import pickle
import sys
import numpy as np
import matplotlib.pyplot as plt

def plot(file_name):
  """ Function to load data and metadata, and create stacked plot """

  file_path = (os.path.abspath(os.path.dirname(__file__))
      + '/test_configs/' + file_name)

  # check file existence
  if not (os.path.isfile(file_path + '.npy')
      or os.path.isfile(file_path + '.pkl')):
    print "Data not found, run test on conf file first!"
    return

  # load data
  data = np.load(file_path + '.npy')
  plkData = file_path + '.pkl'
  with open(plkData, 'rb') as f:
    metadata = pickle.load(f)

  # calculate utilisation
  utilisation = 0
  for point in np.nditer(data):
    utilisation += point
  utilisation /= len(data) * metadata['processors_limit']

  # plot graph
  cycles = np.arange(0, len(data))
  plt.stackplot(cycles, np.transpose(data), labels = metadata['machinetypes'])

  plt.title('Utilisation: {}%'.format(100*utilisation))

  plt.legend(loc = 4)
  plt.xlabel('Cycle')
  plt.ylabel('Machine count')

  plt.savefig(file_path + '.pdf')

if __name__ == '__main__':
  plot(sys.argv[1])
