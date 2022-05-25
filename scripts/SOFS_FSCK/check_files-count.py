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
     .appName("Count SPARSE FILES:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


# files = "file:///%s/listkeys-%s.csv" % (PATH, RING)
files = "%s://%s/%s/listkeys.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.filter( df["_c1"].rlike(r".*0801000040$") )
df = df.groupBy("_c1").count()
df.show(20,False)
print((df.count(), len(df.columns)))
