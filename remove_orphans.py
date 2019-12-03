import os
import time
import requests
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

RING = sys.argv[1]
spark = SparkSession.builder.appName("Remove Orphans ring:"+RING).getOrCreate()

def deletekey(row):
	key = row._c0
        r = requests.delete('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)))
	return ( key, r.status_code)

files = "file:///fs/spark/output/output-spark-ARCORPHAN-%s.csv" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
rdd = df.rdd.map(deletekey).toDF()
rdd.show(10,False)
deletedorphans = "file:///fs/spark/output/output-spark-DELETED-ARCORPHAN-%s.csv" % RING
rdd.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)
