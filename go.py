import risk_DSconvert as ds
import risk_SparkContextFactory as scf

import ComputeCovHistory as comp

from datetime import datetime

def go():
  startTime = datetime.now()

  converter = ds.dsConvert()
  converter.setInputFile("C:\Users\Howard Xie\Desktop\Risk\dev.mat")

  # reading data elements from MAT file
  ret_m = converter.MATreader("testM") # factor return AND/OR alpha matrix
  date_m = converter.MATreader("DatesModel")
  lambda_m = converter.MATreader("Lambda")
  startD_m = converter.MATreader("StartDate")
  endD_m = converter.MATreader("EndDate")
  roll_m = converter.MATreader("RollingPeriod")
  startS_m = converter.MATreader("StartStress")
  endS_m = converter.MATreader("EndStress")
  qualcov_m = converter.MATreader("QualCov")

  # convert mat data objs to numpy array
  ret = converter.h5py_2_numpy(ret_m, 1)
  date = converter.h5py_2_numpy(date_m, 4)
  lambd = converter.h5py_2_numpy(lambda_m, 2)
  startD = converter.h5py_2_numpy(startD_m, 3)
  endD = converter.h5py_2_numpy(endD_m, 3)
  roll = converter.h5py_2_numpy(roll_m, 2)
  startS = converter.h5py_2_numpy(startS_m, 3)
  endS = converter.h5py_2_numpy(endS_m, 3)
  qualcov = converter.h5py_2_numpy(qualcov_m, 1)

  converter.closeMAT()

  # instantiate Spark Context and SparkSQL context objs
  scFactory = scf.SparkContextFactory()

  # call maing function with data from MAT file
  result = comp.ComputeCovHistory(scFactory.sc,
       ret, date, startD, endD, lambd, roll, startS, endS, qualcov)

  # Clear Spark Context and SparkSQL Context
  scFactory.disconnect()

  converter.numpy_2_mat(result, "out.mat")
  
  endTime = datetime.now()
  print ("time took in seconds: " +
         str((endTime - startTime).total_seconds()))


go()
