import ewstats as ew
import numpy

def ewstatswrap(sc, RetSeries, DecayFactor = None, WindowLength = None):
  size = RetSeries.shape
  NumObs = size[0]
  NumSeries = size[1]

  # return value initilization
  ERet = numpy.zeros(NumSeries)
  ECov = numpy.zeros((NumSeries, NumSeries))
  Neff = numpy.zeros((NumSeries, NumSeries))

  # default values for non-required input arguments
  if WindowLength is None:
    WindowLength = NumObs
  if DecayFactor is None:
    DecayFactor = 1

  # build lists of numpy matrices (with custom wrapper), then RDD the list
  vectorList = []
  pairList = []

  for i in range(0, NumSeries - 1):
    # for each column/factor, need to calculate estimated expected return
    WindowUsed = WindowLength
    vectorI = RetSeries[:,i]
    if WindowLength > elim_NaN_rows(vectorI).shape[0]:
      WindowUsed = elim_NaN_rows(vectorI).shape[0]
    vectorList.append(ew.distributedEwstats(elim_NaN_rows(vectorI),
	              DecayFactor, WindowUsed))

    # pairwise covariance needed between column/factor pairs
    for j in range(0, NumSeries - 1):
      PairwiseRetSeries = elim_NaN_rows(
                          numpy.column_stack((vectorI, RetSeries[:,j])))
      WindowUsed = WindowLength
      if WindowLength > PairwiseRetSeries.shape[1]:
        WindowUsed = PairwiseRetSeries.shape[1]
      pairList.append(ew.distributedEwstats(PairwiseRetSeries,
	              DecayFactor, WindowUsed))
  
  #print "\n\n unit tests start \n\n"
  #RetSeries_test(vectorList)
  #RetSeries_test(pairList)
 
  distributeCompute(vectorList, sc)
  distributeCompute(pairList, sc)


def distributeCompute(numpyList, sc):
  ewdRDD = sc.parallelize(numpyList) # build RDD
  resultRDD = ewdRDD.map(lambda ewd : ewd.ewstats())
  print resultRDD.take(resultRDD.count())


def elim_NaN_rows(npa):
  if len(npa.shape) == 1: # for 1-D vectors
    return npa[~numpy.isnan(npa)] # ~ inverts true/false
  else: 
    return npa[~numpy.isnan(npa).any(axis=1)] # ~ inverts true/false


def RetSeries_test(ewstatsObjList):
  for obj in ewstatsObjList:
    print obj.RetSeries
    print "one object done"


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
  print type(a.take(a.count()))

