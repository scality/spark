import os
import time
import requests
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

spark = SparkSession.builder.appName("Remove Orphans").getOrCreate()

def deletekey(row):
	key = row._c0
        r = requests.delete('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)))
	return ( key, r.status_code)

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("file:///fs/spark/output/output-spark-ARCORPHAN.csv")
rdd = df.rdd.map(deletekey).toDF()
rdd.show(10,False)
deletedorphans = "file:///fs/spark/output/output-spark-DELETED-ARCORPHAN.csv"
rdd.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)
