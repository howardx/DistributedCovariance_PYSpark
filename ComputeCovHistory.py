import ewstatswrap as ew
import numpy

from datetime import datetime

def ComputeCovHistory(sc, Rtns, DatesModel, StartDate, EndDate, Lambda,
                      RollingPeriod, StartStress, EndStress,
		      QualCov = None):
  StartDateLoc = DatesModel.index(StartDate)
  EndDateLoc = DatesModel.index(EndDate)

  # Define storage device for return
  # matlab zeros() and numpy.zeros() use reversed index for initialization
  Cov = numpy.zeros((EndDateLoc - StartDateLoc + 1, 4, Rtns.shape[1],
	             Rtns.shape[1]))

  # python's range() is "[start, end)" - non-inclusive for "end"
  for t in range(StartDateLoc + 1, EndDateLoc + 2):
    loopstart = datetime.now()
    RtnsActiveDate = Rtns.take([range(0, t)], axis = 0)[0]
    r, Cov[t - StartDateLoc - 1, 1, :, :], e = ew.ewstatswrap(sc,
                                    RtnsActiveDate)
    r, Cov[t - StartDateLoc - 1, 2, :, :], e = ew.ewstatswrap(sc,
                                    RtnsActiveDate, Lambda)
    r, Cov[t - StartDateLoc - 1, 3, :, :], e = ew.ewstatswrap(sc,
                                    RtnsActiveDate, 1, RollingPeriod)
    if QualCov is None:
      Cov[t - StartDateLoc - 1, 0, :, :] = QualCov
    else:
      Cov[t - StartDateLoc - 1, 0, :, :] = Cov[t - StartDateLoc - 1,
		                               1, :, :]

    loopend = datetime.now()
    print ("\n\n\nTime to run a single iteration of loop = " +
         str((loopend - loopstart).total_seconds()) + " seconds")

  return Cov
