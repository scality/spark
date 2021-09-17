import os
import time
import requests
import yaml
import re
import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext
import pyspark.sql.functions as F     

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
PROT = cfg["protocol"]
ACCESS_KEY = cfg['s3']['access_key']
SECRET_KEY = cfg['s3']['secret_key']
ENDPOINT_URL = cfg['s3']['endpoint']
srebuildd_ip  = cfg["srebuildd_ip"]
srebuildd_path  = cfg["srebuildd_double_path"]
srebuildd_url = "http://%s:81/%s" % ( srebuildd_ip, srebuildd_path)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p3.py:Compute the total sizes to be deleted :"+RING) \
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

def statkey(row):
    key = row._c0
    try:
        url = "%s/%s" % ( srebuildd_url, str(key.zfill(40)) )
        r = requests.head(url)
        if r.status_code == 200:
            # print("Status code: " + str(r.status_code))
            size = r.headers.get("X-Scal-Size",False)
            return ( key, r.status_code, size)
        else:
            # print("Status code: " + str(r.status_code))
            return ( key, r.status_code, 0)
    except requests.exceptions.ConnectionError as e:
        return ( key, "HTTP_ERROR", 0)


files = "%s://%s/output/s3fsck/output-s3objects-missing-ring-%s.csv" % (PROT, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
rdd = df.rdd.map(statkey)

#rdd1 = rdd.toDF()

size_computed= rdd.map(lambda x: (2,int(x[2]))).reduceByKey(lambda x,y: x + y).collect()[0][1]
string = "The total computed size of the not indexed RING 0 keys is: %d bytes" % size_computed
print(string)
string = "The total estimated size of all not indexed RING keys is: %d bytes" % (size_computed*12)
print(string)

#totalsize = "file:///%s/output/s3fsck/output-size-computed-%s.csv" % (PATH, RING)
#rdd1.write.format('csv').mode("overwrite").options(header='false').save(totalsize)
