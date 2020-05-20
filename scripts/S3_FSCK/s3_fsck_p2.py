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
     .appName("s3_fsck_p2.py:Union the S3 keys and the RING keys :"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


s3keys = "file:///fs/spark/output/s3fsck/s3-dig-keys-%s.csv" % RING
ringkeys = "file:///fs/spark/output/s3fsck/input-arc-%s-keys.csv" % RING
dfs3keys = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3keys)
dfringkeys =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ringkeys)

dfringkeys = dfringkeys.withColumnRenamed("_c1","digkey")

#inner_join_true =  dfs3keys.join(dfringkeys,["digkey"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('key','subkey','is_present','digkey')
#inner_join_false =  dfs3keys.join(dfringkeys,["digkey"], "leftanti").withColumn('is_present', F.lit(int(0))).select('key','subkey','is_present','digkey')

inner_join_true =  dfringkeys.join(dfs3keys,["digkey"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('ringkey','is_present','digkey')
inner_join_false =  dfringkeys.join(dfs3keys,["digkey"], "leftanti").withColumn('is_present', F.lit(int(0))).select('ringkey','is_present','digkey')

print inner_join_true.show(20,False)
print inner_join_false.show(20,False)

df_final = inner_join_true.union(inner_join_false)
all = "file:///fs/spark/output/s3fsck/output-s3objects-df_final-%s.csv" % RING
df_final.write.format('csv').mode("overwrite").options(header='true').save(all)

"""
print df_final.show(10,False)

df_all = df_final.groupBy("key").agg(F.sum('is_present').alias('sum'),F.count('is_present').alias('count'))

print df_all.show(10,False)

columns_to_drop = ['count','sum']
df_final_all = df_all.withColumn('good_state', F.when( ( F.col("sum") == F.col("count") ),True).otherwise(False)).drop(*columns_to_drop)

print df_final_all.show(10,False)

all = "file:///fs/spark/output/s3fsck/output-s3objects-%s.csv" % RING
df_final_all.write.format('csv').mode("overwrite").options(header='true').save(all)

"""
