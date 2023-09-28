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


# s3keys are read from the verifySproxydKeys.js scripts output
s3keys = "%s://%s/%s/s3fsck/s3-dig-keys.csv" % (PROTOCOL, PATH, RING)
# ringkeys are read from the listkeys.py (or ringsh dump) scripts output
ringkeys = "%s://%s/%s/s3fsck/arc-keys.csv" % (PROTOCOL, PATH, RING)

# reading with a header, the columns are named. The column _c1 will be whatever column the _c1 header is assigned to
dfs3keys = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3keys)
# reading with a header, the columns are named. The column _c1 will be whatever column the _c1 header is assigned to
dfringkeys =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ringkeys)

# rename the column _c1 to digkey, the next write will output a header that uses digkey instead of _c1
dfringkeys = dfringkeys.withColumnRenamed("_c1","digkey")

# inner join the s3keys and ringkeys on the digkey column
# the result will be a dataframe with the columns ringkey, digkey
# the leftanti option will remove the rows that are present in both dataframes.
inner_join_false =  dfringkeys.join(dfs3keys,["digkey"], "leftanti").withColumn("is_present", F.lit(int(0))).select("ringkey", "is_present", "digkey")

# Create the final dataframe with only the ringkey column
df_final = inner_join_false.select("ringkey")

# write the final dataframe to a csv file
allmissing = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
df_final.write.format("csv").mode("overwrite").options(header="false").save(allmissing)

