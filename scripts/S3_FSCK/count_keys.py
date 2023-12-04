from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import yaml

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
COS = cfg["cos_protection"]

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p1.py:Build RING keys :" + RING) \
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
     .config("spark.driver.host", "10.132.183.52") \
     .config("spark.driver.port", "10027") \
     .getOrCreate()


listkeys = "%s://%s/%s/listkeys.csv" % (PROTOCOL, PATH, RING)
lines_listkeys = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(listkeys)
lines_listkeys_count = lines_listkeys.count()

digkeys = "%s://%s/%s/s3fsck/arc-keys.csv" % (PROTOCOL, PATH, RING)
lines_digkeys = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(digkeys)
lines_digkeys_count = lines_digkeys.count()

s3keys = "%s://%s/%s/s3-bucketd" % (PROTOCOL, PATH, RING)
from pyspark.sql.functions import input_file_name
lines_s3keys = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(s3keys + "/*") #.filter(~input_file_name().startswith("bseq"))
lines_s3keys_count = lines_s3keys.count()

s3digkeys = "%s://%s/%s/s3fsck/s3-dig-keys.csv" % (PROTOCOL, PATH, RING)
lines_s3digkeys = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(s3digkeys)
lines_s3digkeys_count = lines_s3digkeys.count()

print("==== listkeys: %s digkeys: %s ====" % (lines_listkeys_count, lines_digkeys_count))
print("==== s3keys: %s s3digkeys: %s ====" % (lines_s3keys_count, lines_s3digkeys_count))

spark.stop()
