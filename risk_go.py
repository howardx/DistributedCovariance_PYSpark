import risk_DSconvert as ds
import risk_SparkContextFactory as scf
import ewstatsRDD as ewr

import ewstatswrap as eww

def go():
  converter = ds.dsConvert()
  converter.setInputFile("C:\Users\Howard Xie\Desktop\Risk\dev.mat")

  matdata = converter.MATreader("testM")

  testM = converter.h5py_2_numpy(matdata) # numpy
  converter.closeMAT()

  scFactory = scf.SparkContextFactory()

  indexRowRDD = converter.numpy_2_indexRowRDD(scFactory.sc, testM) # RDD

  # take indexedRow from indexedRowRDD based on ordering of the first
  # element of the row element - a tuple
  #[ERet, ECov, NEff] = ewr.ewstats(indexRowRDD, .6, 4,
  #                  input_dim = testM.shape)


  # test with distributed computing
  eww.ewstatswrap(testM, .6, 4, scFactory.sc)


  scFactory.disconnect()

go()
