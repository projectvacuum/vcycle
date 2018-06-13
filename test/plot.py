#! /usr/bin/python2

import os
import sys

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as col
from cycler import cycler


def load_data(file_name):

  file_path = (os.path.abspath(os.path.dirname(__file__))
      + '/test_configs/' + file_name)
  # check file existence
  if not (os.path.isfile(file_path + '.npy')
      or os.path.isfile(file_path + '.pkl')):
    print "Data not found, run test on conf file first!"
    return None

  return np.load(file_path + '.npy')

def color_cycle():
  """Generates a color cycle with 3 shades of each color"""

  def triple_shade(color):
    shd = 0.25 # shade difference, adds/subtracts to give light/dark version
    rgb_org = col.to_rgb(color)
    # tuple because for some reason map changes type to list
    rgb_light = tuple(map(lambda x: x + shd if x < 1 - shd else 1, rgb_org))
    rgb_dark = tuple(map(lambda x: x - shd if x > shd else 0, rgb_org))
    return map(col.to_rgba, [rgb_dark, rgb_org, rgb_light])

  defaults = ['C' + str(i) for i in range(10)] # default colour cycle
  return [x
      for y in map(triple_shade, defaults)
      for x in y]


def plot(file_name):
  """Function to load data and metadata, and create stacked plot"""

  data = load_data(file_name)
  cycles = np.arange(0, len(data)) # x axis

  machinetypes = data.dtype.fields.keys()
  labels = [x + ' - ' + i
      for x in machinetypes
      for i in ['starting', 'running', 'stopping']]

  # This gets out starting, running and stopping
  def extractor(struct):
    entry = np.empty(3*len(struct), dtype='int_')
    i=0
    for x in struct:
      entry[i], entry[i+1], entry[i+2] = x[1], x[2], x[3]
      i+=3
    return entry
  plotting_data = np.array([extractor(x) for x in data])

  utilisation = 0
  for x in plotting_data:
    utilisation += sum(x[i] for i in [1, 2, 4, 5])
  util_percent = 100*utilisation/float(len(data)*1000)

  # colour
  plt.rc('axes', prop_cycle=cycler('color', color_cycle()))

  # plot graph
  plt.stackplot(cycles, np.transpose(plotting_data), labels = labels)

  plt.title('Utilisation (running + stopping): {}%'.format(util_percent))

  plt.legend(loc = 4)
  plt.xlabel('Cycle')
  plt.ylabel('Machine count')

  plt.show()

if __name__ == '__main__':
  plot(sys.argv[1])
