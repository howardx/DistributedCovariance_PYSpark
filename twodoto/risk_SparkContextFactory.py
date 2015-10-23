#!/usr/local/bin/python2.7

from operator import add
import os, sys

#####################################################################
# should ONLY be accessed by SparkContextFactory class
# as this class should be singleton
#####################################################################
class SparkContextFactory:
  def __init__(self):
    # not sure why windows environment variable can't be read, I set it 
    ##os.environ["SPARK_HOME"] = "C:\Spark"
    # not sure why windows environment variable can't be read, I set it 
    ##os.environ["HADOOP_CONF_DIR"] = "C:\hdp\bin"
    ##sys.path.append("C:\Spark\python")
    ##sys.path.append("C:\Spark\bin")

    # specify spark home
    os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark"
    # specify pyspark path so its libraries can be accessed by this application
    sys.path.append("/opt/cloudera/parcels/CDH-5.4.4-1.cdh5.4.4.p0.4/lib/spark/python")
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext

    self.conf = SparkConf().setMaster("yarn-client")
    self.conf.setAppName("MrT")
    self.conf.set("spark.executor.memory", "5g")
    self.conf.set("spark.driver.memory", "10g")

    self.sc = SparkContext(conf = self.conf, pyFiles =
    ["ComputeCovHistory.py", "go.py", "risk_DSconvert.py", "ewstats.py", "ewstatsRDD.py", "ewstatswrap.py"])

    """
    toDF method is a monkey patch executed inside SQLContext constructor
    so to be able to use it you have to create a SQLContext first
    """
    self.sqlContextInstance = SQLContext(self.sc)


  def disconnect(self):
    self.sc.stop()
