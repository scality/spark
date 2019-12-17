import os
import time
import requests
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

RING = sys.argv[1]
PATH = "/fs/spark"
srebuildd_ip = "127.0.0.1"
srebuildd_url = "http://%s:81/rebuild/arcdata/" % srebuildd_ip

spark = SparkSession.builder \
     .appName("Check Orphans ring:"+RING) \
     .config("spark.executor.memory", "60g") \
     .config("spark.driver.memory", "50g") \
     .config("spark.memory.offHeap.enabled",True) \
     .config("spark.memory.offHeap.size","16g") \
     .getOrCreate()

def getarcid(row):
    key = row._c1
    header = {}
    header['x-scal-split-policy'] = "raw"
    try:
	    r = requests.head(srebuildd_url+str(key.zfill(40)),timeout=10)
	    if r.status_code == 200:
		return (key,"OK")
	    elif r.status_code == 422:
		return (key,"CORRUPTED")
	    else:
		return(key,"UNKNOWN|RING_FAILURE|SREBUILDD_DOWN")
    except requests.exceptions.ConnectionError as e:
        return (key,"ERROR_HTTP")

files = "file://%s/listkeys-%s.csv/" % ( PATH , RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

#check only not deleted KEYS ?
#dfARC = df.filter( df["_c1"].rlike(r".*70$") &  (df["_c3"] != 1) )
#check all the keys 
dfARC = df.filter( df["_c1"].rlike(r".*70$"))
dfcARC = dfARC.groupBy("_c1").count().filter("count < 3")

corrupted = dfcARC.rdd.map(getarcid)
corruptednew = corrupted.toDF()

df_final_all = corruptednew.filter(corruptednew["_2"] == "CORRUPTED")

filenamearc = "file://%s/output/output-spark-ARCORPHAN-%s.csv" % (PATH, RING)
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)
