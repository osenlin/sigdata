import pyspark
from pyspark.sql import SparkSession


sl=[]

    #x.armp.map(armp())

class nam:
    def __init__(self,name=None):
        pass
    def sa(self,x):
        print x
    def start(self):
        sparkSession = SparkSession.builder.config(conf=pyspark.SparkConf()).enableHiveSupport().getOrCreate()
        d = [{'name': 'Alice', 'age': 1, "armp": [{"name": "l", "age": "12"}]}]
        s = sparkSession.createDataFrame(d)
        s.rdd.foreach(lambda x: nam().sa(x))


