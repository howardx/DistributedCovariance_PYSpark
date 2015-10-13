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


  # test with distributed computing
  eww.ewstatswrap(scFactory.sc, testM, .6, 4)


  scFactory.disconnect()

go()
