from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests
import yaml

config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")

# USED FOR TESTING IN PYTHON CONSOLE
# config_path = "%s/%s" % ( sys.path[0], "/root/spark/scripts/config/config.yml")

with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) > 1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]


PATH = cfg["path"]

PROTOCOL = cfg["protocol"]

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


digkeys = "%s://%s/%s/s3fsck/s3-dig-keys.csv" % (PROTOCOL, PATH, RING)

orphans = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)

df_p0 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(digkeys)

df_p2_tmp = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(orphans)

df_p2_renamed = df_p2_tmp.withColumnRenamed("_c0", "p2_ringkey")

df_p2 = df_p2_renamed.withColumn("p2_digkey", F.substring("p2_ringkey", 0, 26))

df_final = df_p2.join(df_p0, df_p2['p2_digkey'] == df_p0['digkey']).select(df_p2_renamed['p2_ringkey'])

allmissing = "%s://%s/%s/s3fsck/verification/compare_p2_to_p0.csv" % (PROTOCOL, PATH, RING)
df_final.write.format("csv").mode("overwrite").options(header="false").save(allmissing)
