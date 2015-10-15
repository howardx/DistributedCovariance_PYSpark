import ewstatswrap as eww
import numpy

def ComputeCovHistory(sc, Rtns, DatesModel, StartDate, EndDate, Lambda,
                      RollingPeriod, StartStress, EndStress, QualCov):
  StartDateLoc = DatesModel.index(StartDate)
  EndDateLoc = DatesModel.index(EndDate)

  RtnsActiveDate = Rtns.take([range(0, StartDateLoc + 1)], axis = 0)[0]

  # test with distributed computing
  eww.ewstatswrap(sc, RtnsActiveDate)

