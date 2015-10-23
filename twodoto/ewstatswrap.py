#!/usr/local/bin/python2.7

import ewstats as ew
import numpy
import itertools

def ewstatswrap(sc, RetSeries, DecayFactor = None, WindowLength = None):
  numpy.set_printoptions(suppress = True)
  
  size = RetSeries.shape
  NumObs = size[0]
  NumSeries = size[1]

  # default values for non-required input arguments
  if WindowLength is None:
    WindowLength = NumObs
  if DecayFactor is None:
    DecayFactor = 1

  # build lists of numpy matrices (with custom wrapper), then RDD the list
  vectorList = []
  pairList = []

  for i in range(0, NumSeries):
    # for each column/factor, need to calculate estimated expected return
    WindowUsed = WindowLength
    vectorI = RetSeries[:,i]
    if WindowLength > elim_NaN_rows(vectorI).shape[0]:
      WindowUsed = elim_NaN_rows(vectorI).shape[0]
    vectorList.append(ew.distributedEwstats(elim_NaN_rows(vectorI),
	              DecayFactor, WindowUsed))

    # pairwise covariance needed between column/factor pairs
    for j in range(0, NumSeries):
      PairwiseRetSeries = elim_NaN_rows(
                          numpy.column_stack((vectorI, RetSeries[:,j])))
      WindowUsed = WindowLength
      if WindowLength > PairwiseRetSeries.shape[0]:
        WindowUsed = PairwiseRetSeries.shape[0]
      pairList.append(ew.distributedEwstats(PairwiseRetSeries,
	              DecayFactor, WindowUsed))
  
  # checking input lists which will be used for RDD generation
  #RetSeries_test(vectorList)
  #RetSeries_test(pairList)

  vectorResultList = distributeCompute(vectorList, sc)
  pairResultList = distributeCompute(pairList, sc)

  return aggregateResults(vectorResultList, pairResultList, NumSeries)


def aggregateResults(vectorResult, pairResult, NumSeries):
  # return value initilization
  ERet = numpy.empty([NumSeries,])
  ECov = numpy.empty([NumSeries, NumSeries])
  Neff = numpy.empty([NumSeries, NumSeries])

  # colum/cell index generator
  covCol = itertools.cycle(range(0, NumSeries))
  effCol = itertools.cycle(range(0, NumSeries))
  covColIndex = covCol.next()
  effColIndex = effCol.next()

  for (x, y), result in numpy.ndenumerate(vectorResult):
    if y == 0: # only getting first column/cell of each row
      ERet[x,] = result

  for (x, y), result in numpy.ndenumerate(pairResult):
    if y == 1: # second column/cell of each row - cov
      ECov[x%NumSeries, covColIndex] = result[1][0]
      if x%NumSeries == (NumSeries - 1):
        covColIndex = covCol.next()

    if y == 2: # third column/cell of each row - neff 
      Neff[x%NumSeries, effColIndex] = result
      if x%NumSeries == (NumSeries - 1):
        effColIndex = effCol.next()

  return ERet, ECov, Neff 


def distributeCompute(numpyList, sc):
  ewdRDD = sc.parallelize(numpyList) # build RDD
  resultRDD = ewdRDD.map(lambda ewd : ewd.ewstats())
  return numpy.array(resultRDD.take(resultRDD.count()))


def elim_NaN_rows(npa):
  if len(npa.shape) == 1: # for 1-D vectors
    return npa[~numpy.isnan(npa)] # ~ inverts true/false
  else: 
    return npa[~numpy.isnan(npa).any(axis=1)] # ~ inverts true/false


def RetSeries_test(ewstatsObjList):
  for obj in ewstatsObjList:
    print obj.RetSeries


##############################################################################

def ewstatswrapPOC(sc, RetSeries, DecayFactor = None, WindowLength = None):
  # generate objects
  ewd1 = ew.distributedEwstats(RetSeries, 1, 2)  
  ewd2 = ew.distributedEwstats(RetSeries, .5, 4)  
  ewd3 = ew.distributedEwstats(RetSeries, .6, 3)

  ewdList = [ewd1, ewd2, ewd3]
  ewdRDD = sc.parallelize(ewdList) # build RDD
  a = ewdRDD.map(lambda ewd : ewd.ewstats())

  print a.take(a.count())
