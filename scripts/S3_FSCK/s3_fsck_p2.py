from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests

import yaml

config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
PROT = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p2.py:Union the S3 keys and the RING keys :" + RING) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
     .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
     .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)\
     .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


s3keys = "%s://%s/output/s3fsck/s3-dig-keys-%s.csv" % (PROT, PATH, RING)
ringkeys = "%s://%s/output/s3fsck/input-arc-%s-keys.csv" % (PROT, PATH, RING)
dfs3keys = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3keys)
dfringkeys =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ringkeys)

dfringkeys = dfringkeys.withColumnRenamed("_c1","digkey")

inner_join_false =  dfringkeys.join(dfs3keys,["digkey"], "leftanti").withColumn("is_present", F.lit(int(0))).select("ringkey", "is_present", "digkey")
df_final = inner_join_false.select("ringkey")
allmissing = "%s://%s/output/s3fsck/s3objects-missing-ring-%s.csv" % (PROT, PATH, RING)
df_final.write.format("csv").mode("overwrite").options(header="false").save(allmissing)
