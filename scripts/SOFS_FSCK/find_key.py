"""
find_key.py:
output:%RING/sofs-FIND-SPARSE-%key.csv
"""
from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkContext
import sys
import yaml
import requests
import json



config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]
PROTOCOL = cfg["protocol"]


spark = SparkSession.builder \
     .appName("find SPARSE keys:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


key = "0A5EFDBF85DD192E6ABF5A000000000801000040"
# files = "file:///%s/output/output-sofs-files-%s.csv" % (PATH , RING)
files = "%s://%s/%s/sofs-files.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(files)
df = df.filter( df["key"].rlike(r""+key) )
df.show(20,False)

# single = "file:///%s/output/output-sofs-FIND-SPARSE-%s-%s.csv" % (PATH , RING, key)
single = "%s:///%s/%s/sofs-FIND-SPARSE-%s.csv" % (PROTOCOL, PATH, RING, key)
df.write.format('csv').mode("overwrite").options(header='true').save(single)
