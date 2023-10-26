import os
import time
import requests
import yaml
import re
import sys
import struct
import base64
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext


config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

USER = cfg["sup"]["login"]
PASSWORD = cfg["sup"]["password"]
URL = cfg["sup"]["url"]
PATH = cfg["path"]
SREBUILDD_IP  = cfg["srebuildd_ip"]
SREBUILDD_PATH  = cfg["srebuildd_chord_path"]
SREBUILDD_URL = "http://%s:81/%s" % (SREBUILDD_IP, SREBUILDD_PATH)
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])
ARC = cfg["arc_protection"]

arcindex = {"4+2": "102060", "8+4": "2040C0", "9+3": "2430C0", "7+5": "1C50C0", "5+7": "1470C0"}
arcdatakeypattern = re.compile(r'[0-9a-fA-F]{38}70')

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
        key = row.ringkey
        try:
                url = "%s/%s" % ( SREBUILDD_URL, str(key.zfill(40)) )
                print(url)
                r = requests.delete(url)
                status_code = r.status_code
                #status_code = "OK"
                return ( key, status_code, url)
        except requests.exceptions.ConnectionError as e:
                return (key,"ERROR_HTTP", url)


files = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)

# reading without a header,
# columns _c0 is the default column names of
# column    1 for the csv
# input structure: _c0 (main chunk)
#   e.g. 998C4DF2FC7389A7C82A9600000000512040C070
# Required Fields:
#   - _c0 (main chunk)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
# rename the column _c0 (column 1) to ringkey
df = df.withColumnRenamed("_c0","ringkey")

# repartition the dataframe to the number of partitions (executors * cores)
df = df.repartition(PARTITIONS)

# map the deletekey function to the dataframe: blindly delete keys on the RING.
rdd = df.rdd.map(deletekey).toDF()

deletedorphans = "%s://%s/%s/s3fsck/deleted-s3-orphans.csv" % (PROTOCOL, PATH, RING)
# write the dataframe to a csv file with the results of the deletekey function
rdd.write.format("csv").mode("overwrite").options(header="false").save(deletedorphans)
