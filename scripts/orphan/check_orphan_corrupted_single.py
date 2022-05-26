import os
import time
import yaml
import requests
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

from scality.key import Key

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
try:
    SREBUILDD_PATH = cfg["srebuildd_arcdata_path"]
except KeyError:
    SREBUILDD_PATH = cfg["srebuildd_path"]

try:
    SREBUILDD_URL = cfg["srebuildd_url"] + "/%s/" % SREBUILDD_PATH
except KeyError:
    # Backward compatibility
    SREBUILDD_URL = "http://%s:81/%s" % (cfg["srebuildd_ip"], SREBUILDD_PATH)

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


def build_rep(row):
    key = Key(row._c0)
    l = [("key",key.getHexPadded())] + [ ("key", k.getHexPadded()) for k in key.getReplicas() ]
    return l

def getarcid(row):
    key = Key(row._2).getHexPadded()
    header = {}
    header['x-scal-split-policy'] = "raw"
    try:
	r = requests.head(srebuildd_url+str(key),timeout=300)
	if r.status_code == 200:
	    return (key,"OK")
	elif r.status_code == 422:
	    return (key,"CORRUPTED")
	else:
	    return(key,"UNKNOWN|RING_FAILURE|SREBUILDD_DOWN")
    except requests.exceptions.ConnectionError as e:
        return (key,"ERROR_HTTP")

files = "file://%s/output/output-spark-ARCORPHAN-CORRUPTED-BUT-OK-%s.csv" % (PATH, RING)

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
print(df.count(), len(df.columns))

listofrepsd = df.rdd.flatMap(build_rep)
listofrepsd = listofrepsd.toDF()
print( listofrepsd.count(), len(listofrepsd.columns) )

corrupted = listofrepsd.rdd.map(getarcid)
corrupted = corrupted.toDF()
print(corrupted.count(), len(corrupted.columns) )

df_final_all = corrupted.filter(corrupted["_2"] != "OK")

filenamearc = "file://%s/output/output-spark-ARCORPHAN-CORRUPTED-BUT-OK-LAST-%s.csv" % (PATH, RING)
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)
