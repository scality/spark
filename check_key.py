import os
import time
import requests
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

key = sys.argv[1]
RING = sys.argv[2]

spark = SparkSession.builder.appName("Check Keys").getOrCreate()
files = "file:///fs/spark/listkeys-%s.csv/" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

dfARC = df.filter( df["_c1"] == key )
dfARC.show(10,False)

