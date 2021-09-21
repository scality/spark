import os
import time
import requests
import yaml
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext


config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
SREBUILDD_IP  = cfg["srebuildd_ip"]
SREBUILDD_PATH  = cfg["srebuildd_single_path"]
#SREBUILDD_PATH  = cfg["srebuildd_double_path"]
SREBUILDD_URL = "http://%s:81/%s" % (SREBUILDD_IP, SREBUILDD_PATH)
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p4.py:Clean the extra keys :" + RING) \
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
     .config("spark.local.dir", PATH) \
     .getOrCreate()

def deletekey(row):
    key = row.objectkey
    try:
        url = "%s/%s" % (SREBUILDD_URL, str(key.zfill(40)))
        print(url)
        r = requests.delete(url)
        status_code = r.status_code
        #status_code = "OK"
        return ( key, status_code, url)
    except requests.exceptions.ConnectionError as e:
        return ( key,"ERROR_HTTP")
missingfiles = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(missingfiles)
df.show()
print "------------------------------------------------------------\n"
print "DF Length: " + str(len(df.show()))
print "------------------------------------------------------------\n"

allkeysfiles = "%s://%s/%s/listkeys.csv" % (PROTOCOL, PATH, RING)
df2 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(allkeysfiles)
df2.show()
print "------------------------------------------------------------\n"
print "DF2 Length: " + str(len(df2.show()))
print "------------------------------------------------------------\n"


df = df.withColumnRenamed("_c0","ringkey")
df2 = df2.withColumnRenamed("_c0","digkey").withColumnRenamed("_c4", "objectkey")
df2 = df2.filter(df2.objectkey.contains("70"))
df2.show()
print "------------------------------------------------------------\n"
print "DF2 filtered Length: " + str(len(df.show()))
print "------------------------------------------------------------\n"
dfnew = df.join(df2, df.ringkey == df2.digkey).select(df["*"],df2["objectkey"])
print "------------------------------------------------------------\n"
print "DFNEW Length: " + str(len(df.show()))
print "------------------------------------------------------------\n"

dfnew = dfnew.drop("ringkey")
# dfnew = dfnew.repartition(4)
dfnew = dfnew.repartition(PARTITIONS)
#dfnew.show(100, False)
rdd = dfnew.rdd.map(deletekey).toDF()
#rdd.show(100, False)
deletedorphans = "%s://%s/%s/s3fsck/deleted-s3-orphans.csv" % (PROTOCOL, PATH, RING)
rdd.write.format("csv").mode("overwrite").options(header="false").save(deletedorphans)
