"""
check_files_p3.py:Compare the DIG and the ARC keys
output:output/output-sofs-SPARSE-FILE-SHAPE-%RING.csv
"""

from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests
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
     .appName("check_files_p3.py:Compare the dig ARC keys with the RING ARC keys:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()



sparse = "file:///%s/output/output-sparse-ARC-FILES-%s.csv" % (PATH, RING)
dig = "file:///%s/output/output-sofs-files-DIG-%s.csv" % (PATH, RING)
dfsparse = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(sparse)
dfdig =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dig)

dfsparse = dfsparse.withColumnRenamed("_c1","arckey")


inner_join_true =   dfdig.join(dfsparse,["arckey"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('key','subkey','is_present')
inner_join_false =  dfdig.join(dfsparse,["arckey"], "leftanti").withColumn('is_present', F.lit(int(0))).select('key','subkey','is_present')

print inner_join_true.show(20,False)
print inner_join_false.show(20,False)

df_final = inner_join_true.union(inner_join_false)
print df_final.show(10,False)

df_all = df_final.groupBy("key").agg(F.sum('is_present').alias('sum'),F.count('is_present').alias('count'))

columns_to_drop = ['count','sum']
df_final_all = df_all.withColumn('good_state', F.when( ( F.col("sum") == F.col("count") ),True).otherwise(False)).drop(*columns_to_drop)

print df_final_all.show(10,False)

all = "file:///fs/spark/output/output-sofs-SPARSE-FILE-SHAPE-%s.csv" % RING
df_final_all.write.format('csv').mode("overwrite").options(header='true').save(all)

