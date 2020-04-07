import os
import time
import yaml
import requests
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
     .appName("Check Orphans Corrupted ring:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

def getarcid(row):
    key = row._c0
    header = {}
    header['x-scal-split-policy'] = "raw"
    try:
	    r = requests.head(srebuildd_url+str(key.zfill(40)),timeout=300)
	    if r.status_code == 200:
		return (key,"OK")
	    elif r.status_code == 422:
		return (key,"CORRUPTED")
	    elif r.status_code == 404:
		return (key,"NOTFOUND")
	    else:
		return(key,"UNKNOWN|RING_FAILURE|SREBUILDD_DOWN")
    except requests.exceptions.ConnectionError as e:
        return (key,"ERROR_HTTP")

files = "file://%s/output/output-spark-ARCORPHAN-%s.csv" % (PATH, RING)

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

df.rdd.getNumPartitions()
corrupted = df.rdd.map(getarcid)
corruptednew = corrupted.toDF()

df_final_all = corruptednew.filter(corruptednew["_2"] != "OK")
df_final_ok = corruptednew.filter(corruptednew["_2"] == "OK")

filenamearc = "file://%s/output/output-spark-ARCORPHAN-CORRUPTED-%s.csv" % (PATH, RING)
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)
filenamearc = "file://%s/output/output-spark-ARCORPHAN-CORRUPTED-BUT-OK-%s.csv" % (PATH, RING)
df_final_ok.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)
