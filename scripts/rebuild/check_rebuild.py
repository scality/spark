import os
import time
import yaml
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]
srebuildd_ip  = cfg["srebuildd_ip"]
srebuildd_url = "http://%s:81/rebuild/arcdata/" % srebuildd_ip

spark = SparkSession.builder \
     .appName("Check Keys to be rebuilt:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
      .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

files = "file://%s/listkeys-%s.csv/" % ( PATH , RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

dfARC = df.filter( df["_c1"].rlike(r".*70$"))
dfcARC = dfARC.groupBy("_c1").count().filter("count < 6")

filenamearc = "file://%s/output/output-spark-KEYS-TO-BE-REBUILT-%s.csv" % (PATH, RING)
dfcARC.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)
