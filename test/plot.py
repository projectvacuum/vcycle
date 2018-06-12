import os
import pickle
import sys
import numpy as np
import matplotlib.pyplot as plt

def plot(file_name):
  data = np.load(file_name)
  cycles = np.arange(0, len(data))

  plkData = os.path.abspath(
      os.path.join(os.path.dirname(__file__), file_name[:-3] + 'pkl'))
  with open(plkData, 'rb') as f:
    metadata = pickle.load(f)

  utilisation = 0
  for point in np.nditer(data):
    utilisation += point

  utilisation /= len(data) * metadata['processors_limit']
  plt.title('Utilisation: {}%'.format(100*utilisation))

  plt.stackplot(cycles, np.transpose(data), labels = metadata['machinetypes'])

  plt.legend(loc = 2)
  plt.xlabel('Cycle')
  plt.ylabel('Machine count')

  plt.show()

if __name__ == '__main__':
  plot(sys.argv[1])
