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


# s3keys are generated by verifySproxydKeys.js script and processed by s3_fsck_p0.py
s3keys = "%s://%s/%s/s3fsck/s3-dig-keys.csv" % (PROTOCOL, PATH, RING)
# ringkeys are generated by the listkeys.py (or ringsh dump) script and processed by s3_fsck_p1.py
ringkeys = "%s://%s/%s/s3fsck/arc-keys.csv" % (PROTOCOL, PATH, RING)

# reading with a header, the columns are named.
# columns digkey, sproxyd input key, subkey are the actual column names of
# columns      1,                 2,      3 for the csv
# input structure: (digkey, sproxyd input key, subkey)
#   e.g. 7359114991482315D0A5890000,BDE4B9BBEB45711EC2F1A9C78F6BCD59E02C6220,SINGLE
# Required Fields: 
#  - digkey
#  - sproxyd input key
dfs3keys = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3keys)


# reading with a header, the columns are named.
# columns _c1, count, ringkey (main chunk) are the actual column names of
# columns   1,     2,                    3 for the csv
# input structure: (digkey, count, ringkey (main chunk))
#   e.g. 907024530554A8DB3167280000,12,907024530554A8DB31672800000000512430C070
# Required Fields:
#  - digkey
#  - ringkey (main chunk)
dfringkeys =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ringkeys)

# rename the column _c1 to digkey, the next write will output a header that uses digkey instead of _c1
# digkey: the unique part of a main chunk before service id, arc schema, and class are appended
dfringkeys = dfringkeys.withColumnRenamed("_c1","digkey")

# inner join the s3keys (sproxyd input key) and ringkeys (the main chunk of the strip or replica)
# on the digkey column. The result will be a dataframe with the columns ringkey, digkey
# the inner join leftani will not return rows that are present in both dataframes, 
# eliminating ringkeys (main chunks) that have metadata in s3 (not application orphans).
# digkey: the unique part of a main chunk before service id, arc schema, and class are appended
# ringkey: the main chunk of the strip or replica
inner_join_false =  dfringkeys.join(dfs3keys,["digkey"], "leftanti").withColumn("is_present", F.lit(int(0))).select("ringkey", "is_present", "digkey")

# Create the final dataframe with only the ringkey (the main chunk of the strip or replica)
df_final = inner_join_false.select("ringkey")

# write the final dataframe to a csv file
allmissing = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
df_final.write.format("csv").mode("overwrite").options(header="false").save(allmissing)

