import os
import time
import requests
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

spark = SparkSession.builder.appName("Gather ARC Objects").getOrCreate()


RING = sys.argv[1]

files = "file:///fs/spark/listkeys-%s.csv/" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

dfARC = df.filter( df["_c1"].rlike(r".*70$"))
filenamearc = "file:///fs/spark/output/output-spark-ARC-%s.csv" % RING
dfARC.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)

