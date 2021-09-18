import os
import time
import requests
import yaml
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext


config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
SREBUILDD_IP  = cfg["srebuildd_ip"]
#SREBUILDD_PATH  = cfg["srebuildd_single_path"]
SREBUILDD_PATH  = cfg["srebuildd_double_path"]
SREBUILDD_URL = "http://%s:81/%s" % (SREBUILDD_IP, SREBUILDD_PATH)
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p4.py:Clean the extra keys :"+RING) \
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

def deletekey(row):
    key = row._c0
    try:
        url = "%s/%s" % (SREBUILDD_URL, str(key.zfill(40)))
        print(url)
        #r = requests.delete(url)
        #status_code = r.status_code
        status_code = "OK"
        return ( key, status_code, url)
    except requests.exceptions.ConnectionError as e:
        return ( key,"ERROR_HTTP")

files = "%s://%s/output/s3fsck/s3objects-missing-ring-%s.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.repartition(4)
rdd = df.rdd.map(deletekey).toDF()
rdd.show(10,False)
deletedorphans = "%s://%s/output/s3fsck/deleted-s3-orphans-%s.csv" % (PROTOCOL, PATH, RING)
rdd.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)

