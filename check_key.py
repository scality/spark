import os
import time
import requests
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

key = "CMON"

if len(sys.argv)> 1:
	key = sys.argv[1]

spark = SparkSession.builder.appName("Check Keys").getOrCreate()
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("file:///fs/spark/listkeys.csv/")

dfARC = df.filter( df["_c1"] == key )
dfARC.show(10,False)

