import os
import time
import requests
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

RING = sys.argv[1]
PATH = cfg["path"]
PROTOCOL = cfg["protocol"]
spark = SparkSession.builder.appName("Gather ARC Objects ring:"+RING).getOrCreate()

# files = "file:///fs/spark/listkeys-%s.csv/" % RING
files = "%s://%s/%s/listkeys.csv/" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

dfARC = df.filter( df["_c1"].rlike(r".*70$"))
# filenamearc = "file:///fs/spark/output/output-spark-ARC-%s.csv" % RING
filenamearc = "%s://%s/%s/spark-ARC.csv" % (PROTOCOL, PATH, RING)
dfARC.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)

