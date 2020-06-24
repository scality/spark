import os
import time
import requests
import yaml
import re
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
srebuildd_path  = cfg["srebuildd_path"]
srebuildd_url = "http://%s:81/%s/" % ( srebuildd_ip, srebuildd_path)

spark = SparkSession.builder \
     .appName("s3_fsck_p4.py:Clean the extra keys :"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

def deletekey(row):
	key = row._c0
	try:
		url = "%s/%s" % ( srebuildd_url, str(key.zfill(40)) )
		print(url)
		#r = requests.delete(url)
		#status_code = r.status_code
		status_code = "OK"
		return ( key, status_code, url)
	except requests.exceptions.ConnectionError as e:
		return (key,"ERROR_HTTP")

files = "file:///%s/output/s3fsck/output-s3objects-missing-ring-%s.csv" % (PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.repartition(4)
rdd = df.rdd.map(deletekey).toDF()
rdd.show(10,False)
deletedorphans = "file:///%s/output/s3fsck/output-deleted-s3-orphans-%s.csv" % (PATH, RING)
rdd.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)

