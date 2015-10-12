import risk_SparkContextFactory as scf
import numpy
import pandas

import h5py

class dsConvert:
  def setInputFile(self, filename):
    self.matFile = h5py.File(filename, 'r')

  def MATreader(self, dataset):
    data = self.matFile[dataset]
    return data

  def closeMAT(self):
    self.matFile.close()

  def h5py_2_numpy(self, h5pyDataset):
    return numpy.array(h5pyDataset).transpose()

  def numpy_2_pandasDF(self, npArray):
    # have to transpose for h5py
    return pandas.DataFrame(npArray)

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


