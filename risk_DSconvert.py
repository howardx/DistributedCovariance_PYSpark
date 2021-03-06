#!/usr/local/bin/python2.7

import risk_SparkContextFactory as scf
import numpy

try:
  import h5py
  h5py_available = True
except ImportError:
  h5py_available = False
  from scipy import io 

class dsConvert:
  def __init__(self):
    self.h5py_available = h5py_available

  def setInputFile(self, filename):
    if self.h5py_available:
      self.matFile = h5py.File(filename, 'r')
    else:
      self.matFile = io.loadmat(filename)

  def MATreader(self, data):
    d = self.matFile[data]
    return d

  def closeMAT(self):
    if self.h5py_available:
      self.matFile.close()


  # 3 type of data from h5py, 4 approaches
  # 1 - matrix
  # 2 - matrix that actually contains a scalar
  # 3 - 1 dimension char array, contains a string
  # 4 - multi-dimension char array
  def h5py_2_numpy(self, h5pyDataset, dType):
    if dType == 1:
      return numpy.array(h5pyDataset).transpose()
    elif dType == 2:
      mat = numpy.array(h5pyDataset).transpose()
      return mat[0][0] # returnning scalar
    elif dType == 3:
      return u''.join(unichr(c) for c in h5pyDataset)
    elif dType == 4:
      columns = []
      charArray = []
      for array in h5pyDataset:
        columns.append(u''.join(unichr(c) for c in array))

      colSliceM1 = columns[0]
      colSliceM2 = columns[1]
      colSliceM3 = columns[2]
      colSliceY1 = columns[3]
      colSliceY2 = columns[4]
      colSliceY3 = columns[5]
      colSliceY4 = columns[6]

      for i in range (0, len(colSliceM1)):
        charArray.append(''.join([colSliceM1[i], colSliceM2[i],
                         colSliceM3[i], colSliceY1[i], colSliceY2[i],
			 colSliceY3[i], colSliceY4[i]]))
    return charArray


  def numpy_2_indexRowRDD(self, sc, np):
    dimension = np.shape
    indexedRowList = []
    for i in range(0, dimension[0]):
      indexedRowList.append((i, np[i]))
    return sc.parallelize(indexedRowList)

  def indexRowList_2_numpy(self, irl):
    matrix = []
    for indexedRow in irl:
      matrix.append(indexedRow[1])
    return numpy.array(matrix)

  def numpy_2_mat(self, np, filename):
    if self.h5py_available:
      with h5py.File(filename, 'w') as f:
        dset = f.create_dataset("Cov")
      f.close()
    else:
      print np



