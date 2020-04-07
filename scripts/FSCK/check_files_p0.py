from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests
import re
import time
import yaml


config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
        RING = sys.argv[1]
else:
        RING = cfg["ring"]

PATH = cfg["path"]


spark = SparkSession.builder \
     .appName("Check Stripe ARC objects:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()



files = "file:///%s/listkeys-%s.csv" % (PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.filter(df["_c1"].rlike(r".*000000..5.........$") & df["_c3"].rlike("0")).select("_c1")

df.show(20,False)

dfARC = df.filter(df["_c1"].rlike(r".*70$"))
dfARC = dfARC.groupBy("_c1").count().filter("count > 4")

dfARC.show(20,False)

dfARCREP = df.filter(df["_c1"].rlike(r".*20$"))
dfARCREP = dfARCREP.groupBy("_c1").count()

dfARCREP.show(20,False)


dfARC = dfARC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))
dfARCREP = dfARCREP.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))


dfARCALL = dfARC.union(dfARCREP)

dfARCALL.show(20,False)
mainchunk = "file:///%s/output/output-sparse-ARC-FILES-%s.csv" % (PATH, RING)
dfARCALL.write.format('csv').mode("overwrite").options(header='true').save(mainchunk)

