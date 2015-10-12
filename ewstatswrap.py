import ewstats as ew

def ewstatswrap(RetSeries, DecayFactor, WindowLength, sc):
  # generate objects
  ewd1 = ew.distributedEwstats(RetSeries, 1, 2)  
  ewd2 = ew.distributedEwstats(RetSeries, .5, 4)  
  ewd3 = ew.distributedEwstats(RetSeries, .6, 3)

  ewdList = [ewd1, ewd2, ewd3]
  ewdRDD = sc.parallelize(ewdList) # build RDD
  a = ewdRDD.map(lambda ewd : ewd.ewstats())

  print a.take(a.count())
  print type(a.take(a.count()))
