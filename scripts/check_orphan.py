import os
import time
import requests
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

RING = sys.argv[1]
spark = SparkSession.builder.appName("Check Orphans ring:"+RING).getOrCreate()

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

files = "file:///fs/spark/listkeys-%s.csv/" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

dfARC = df.filter( df["_c1"].rlike(r".*70$") &  (df["_c3"] != 1) )
dfcARC = dfARC.groupBy("_c1").count().filter("count < 4")

corrupted = dfcARC.rdd.map(getarcid)
corruptednew = corrupted.toDF()

df_final_all = corruptednew.filter(corruptednew["_2"] == "CORRUPTED")

filenamearc = "file:///fs/spark/output/output-spark-ARCORPHAN-%s.csv" % RING
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)

