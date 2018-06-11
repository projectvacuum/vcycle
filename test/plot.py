import sys
import numpy as np
import matplotlib.pyplot as plt

def plot(file_name):
  data = np.load(file_name)
  cycles = np.arange(0, len(data))
  plt.stackplot(cycles, np.transpose(data))
  plt.xlabel('Cycle')
  plt.ylabel('Machine count')
  plt.show()

if __name__ == '__main__':
  plot(sys.argv[1])
