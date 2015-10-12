from operator import add
import os, sys

#####################################################################
# should ONLY be accessed by SparkContextFactory class
# as this class should be singleton
#####################################################################
class SparkContextFactory:
  def __init__(self):
    # not sure why windows environment variable can't be read, I set it 
    os.environ["SPARK_HOME"] = "C:\Spark"
    # not sure why windows environment variable can't be read, I set it 
    os.environ["HADOOP_CONF_DIR"] = "C:\hdp\bin"
    sys.path.append("C:\Spark\python")
    sys.path.append("C:\Spark\bin")
    
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext
    
    self.conf = (SparkConf().setMaster("local").setAppName("test")
		.set("spark.executor.memory", "1g"))
    self.sc = SparkContext(conf = self.conf)

    """
    toDF method is a monkey patch executed inside SQLContext constructor
    so to be able to use it you have to create a SQLContext first
    """
    self.sqlContextInstance = SQLContext(self.sc)


  def disconnect(self):
    self.sc.stop()
