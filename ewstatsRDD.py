import risk_DSconvert as dsc
import risk_SparkContextFactory as scf

import numpy
import math

def ewstats(RetSeries, DecayFactor, WindowLength = None, **kwargs):
  converter = dsc.dsConvert()
  dimInfo = kwargs['input_dim'] 

  # obtain dimension information of input return matrix
  NumObs = dimInfo[0] # long type
  NumSeries = dimInfo[1] # long type

  # validate input parameters
  if WindowLength is None:
    WindowLength = NumObs
  if DecayFactor <= 0 or DecayFactor > 1:
    print ("finance:ewstats:invalidDecayFactor," +
           "must have 0 < decay factor <= 1")
    return -1
  if WindowLength > NumObs:
    print ("finance:ewstats:invalidWindowLength, Window Length must" +
           "be <= number of observations")
    return -1

  '''
  obtain input data within window only, CONVERTING from a
  DISTRIBUTED spark RDD to a LOCAL python list type
  '''
  RetSeries = RetSeries.top(WindowLength, key =
	        lambda RetSeries: RetSeries[0])
  RetSeries.reverse()
  RetSeries = converter.indexRowList_2_numpy(RetSeries)

  # calculate decay coefficients
  DecayPowers = numpy.arange(WindowLength - 1, -1, -1).reshape(
	        WindowLength, 1)
  VarWts = numpy.power(math.sqrt(DecayFactor), DecayPowers)
  RetWts = numpy.power(DecayFactor, DecayPowers)
 
  # RETURN - number of equivalent values in computation, a scalar
  NEff = numpy.sum(RetWts)

  # compute the exponentially weighted mean return
  WtSeries = numpy.multiply(numpy.repeat(RetWts, NumSeries, axis = 1),
		            RetSeries)
  
  # RETURN - estimated expected return (forward looking in time)
  ERet = WtSeries.sum(axis = 0)/NEff

  # Subtract the weighted mean from the original Series
  CenteredSeries = RetSeries - ERet 

  # compute the weighted variance
  WtSeries = numpy.multiply(numpy.repeat(VarWts, NumSeries, axis = 1),
		            CenteredSeries)

  # 2D matrix multiplication numpy.dot() - dot product equivalent
  # numpy.multiply() does element-wise multiplication
  ECov = numpy.dot(WtSeries.transpose(), WtSeries)/NEff

  # returning:
  # - estimated expected return - ERet
  # - estimated expectedcovariance - ECov
  # - number of effective observation - NEff
  return [ERet, ECov, NEff]


