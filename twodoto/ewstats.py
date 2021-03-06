#!/usr/local/bin/python2.7

import numpy
import math

class distributedEwstats:
  def __init__(self, RetSeries, DecayFactor, WindowLength = None):
    self.DecayFactor = DecayFactor
    self.WindowLength = WindowLength
    self.RetSeries = RetSeries
    
    self.resultList = []
  
  def ewstats(self):
    dimInfo = self.RetSeries.shape 
  
    # obtain dimension information of input return matrix
    NumObs = dimInfo[0] # long type
    if len(dimInfo) == 1:
      NumSeries = 1 # only 1 series - a vector
      '''
      1 D numpy array taken from column will be converted to row-like
      automatically, hence reshape() is needed to convert row-like
      array back to column-like for calculations
      '''
      self.RetSeries = self.RetSeries.reshape(NumObs, 1)
    else:
      NumSeries = dimInfo[1] # long type
  
    # validate input parameters
    if self.WindowLength is None:
      self.WindowLength = NumObs
    if self.DecayFactor <= 0 or self.DecayFactor > 1:
      print ("finance:ewstats:invalidDecayFactor," +
             "must have 0 < decay factor <= 1")
      return -1
    if self.WindowLength > NumObs:
      print ("finance:ewstats:invalidWindowLength, Window Length must" +
             "be <= number of observations")
      return -1
  
    # obtain input data within window only
    self.RetSeries = self.RetSeries[NumObs - self.WindowLength : NumObs, ]

    # calculate decay coefficients
    DecayPowers = numpy.arange(self.WindowLength - 1, -1, -1).reshape(
	          self.WindowLength, 1)
    VarWts = numpy.power(math.sqrt(self.DecayFactor), DecayPowers)
    RetWts = numpy.power(self.DecayFactor, DecayPowers)
   
    # RETURN - number of equivalent values in computation, a scalar
    NEff = numpy.sum(RetWts)
  
    # compute the exponentially weighted mean return
    WtSeries = numpy.multiply(numpy.repeat(RetWts, NumSeries, axis = 1),
		              self.RetSeries)
    
    # RETURN - estimated expected return (forward looking in time)
    ERet = WtSeries.sum(axis = 0)/NEff
  
    # Subtract the weighted mean from the original Series
    CenteredSeries = self.RetSeries - ERet 
  
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
    self.resultList = [ERet, ECov, NEff]
    return self.resultList
  
  
