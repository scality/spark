import os
import time
import requests
import re
import sys
import yaml
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

config_path = "%s/%s" % ( sys.path[0] ,"./config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]
srebuildd_ip  = cfg["srebuildd_ip"]
srebuildd_path  = cfg["srebuildd_path"]
srebuildd_url = "http://%s:81/%s/" % ( srebuildd_ip, srebuildd_path)

spark = SparkSession.builder \
     .appName("Check ring keys:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

def headkey(row):
    key = row._c0

    try:
	    r = requests.head(srebuildd_url+str(key.zfill(40)),timeout=10)
	    if r.status_code in (200,422,404):
		return (key,r.status_code)
	    else:
		return(key,"UNKNOWN|RING_FAILURE|SREBUILDD_DOWN")
    except requests.exceptions.ConnectionError as e:
        return (key,"ERROR_HTTP")


filenamearc = "file://%s/output/output-spark-KEYS-TO-BE-REBUILT-CORRUPTED-%s.csv" % (PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(filenamearc)
rdd = df.rdd.map(headkey).toDF()
rdd.show(10,False)

df_final_ok = rdd.filter(rdd["_2"] != "404")
df_final_ok.show(100,False)

