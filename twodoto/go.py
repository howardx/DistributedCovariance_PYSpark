#!/usr/local/bin/python2.7

import risk_DSconvert as ds
import risk_SparkContextFactory as scf

import ComputeCovHistory as comp

from datetime import datetime

def go():
  startTime = datetime.now()

  converter = ds.dsConvert()
  #converter.setInputFile("/home/howard_xie/risk/MrT/data/dev7.mat")
  converter.setInputFile("/home/howard_xie/risk/MrT/data/RiskTestData.mat")

  # using h5py.File() - only works for -v7.3 mat files
  if ds.h5py_available:
    # reading data elements from MAT file
    ret_m = converter.MATreader("FactorRtns") # factor return AND/OR alpha matrix
    #ret_m = converter.MATreader("Alphas")
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
  # using scipy.loadmat() - only works for -v7 mat files
  else:
    ret = converter.matFile["FactorRtns"] # FactorRtns AND/OR Alphas matrix
    date = converter.matFile["DatesModel"].tolist()
    lambd = converter.matFile["Lambda"][0][0]
    startD = converter.matFile["StartDate"][0]
    endD = converter.matFile["EndDate"][0]
    roll = converter.matFile["RollingPeriod"][0][0]
    startS = converter.matFile["StartStress"][0]
    endS = converter.matFile["EndStress"][0]
    qualcov = converter.matFile["QualCov"]


  # instantiate Spark Context and SparkSQL context objs
  sparkContextLaunchTime = datetime.now()
  scFactory = scf.SparkContextFactory()
  sparkContextUpTime = datetime.now()

  # call main function with data from MAT file
  startComputeTime = datetime.now()
  result = comp.ComputeCovHistory(scFactory.sc,
       ret, date, startD, endD, lambd, roll, startS, endS, qualcov)
  endComputeTime = datetime.now()

  # Clear Spark Context and SparkSQL Context
  scFactory.disconnect()

  converter.numpy_2_mat(result, "out.mat")
 
  endTime = datetime.now()
  print ("Spark Context Launch took in seconds: " +
         str((sparkContextUpTime - sparkContextLaunchTime).total_seconds()))
  print ("Computation Time took in seconds: " +
         str((endComputeTime - startComputeTime).total_seconds()))
  print ("Overall time took in seconds: " +
         str((endTime - startTime).total_seconds()))


go()
