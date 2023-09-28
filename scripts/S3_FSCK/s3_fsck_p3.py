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
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
SREBUILDD_IP  = cfg["srebuildd_ip"]
SREBUILDD_ARCDATA_PATH  = cfg["srebuildd_arcdata_path"]
SREBUILDD_URL = "http://%s:81/%s" % (SREBUILDD_IP, SREBUILDD_ARCDATA_PATH)
ARC = cfg["arc_protection"]
COS = cfg["cos_protection"]

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p3.py:Compute the total sizes to be deleted :" + RING) \
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

# Use of the arcindex limits the inspection to a specific ARC protection scheme.
# If there were more than one cluster with different ARC protection schemes then this would limit the check to a specific scheme.
# FOOD FOR THOUGHT: limits finding keys which may have been written after a schema change or any bug did not honor the schema. 
# The arcindex is a dictionary that contains the ARC protection scheme and the hex value found in the ringkey
arcindex = {"4+2": "102060", "8+4": "2040C0", "9+3": "2430C0", "7+5": "1C50C0", "5+7": "1470C0"}

# The arcdatakeypattern is a regular expression that matches the ARC data keys
arcdatakeypattern = re.compile(r'[0-9a-fA-F]{38}70')


def statkey(row):
    """ statkey takes a row from the dataframe and returns a tuple with the key, status_code, size"""
    key = row._c0
    try:
        url = "%s/%s" % (SREBUILDD_URL, str(key.zfill(40)))
        r = requests.head(url)
        if r.status_code == 200:
            if re.search(arcdatakeypattern, key):  #  Should consider changing this to match any entry in the arcindex
                # The size of the ARC data key is 12 times the size of the ARC index key.
                # At this point there is no longer access to the qty of keys found, so
                # it simply computes based on the presumed schema of 12 chunks per key.
                size = int(r.headers.get("X-Scal-Size", False))*12
            else:
                # The size of the ARC index key is the size of the ARC index key plus the size of the ARC data key times the COS protection.
                # At this point there is no longer access to the qty of keys found, so
                # it simply computes based on the presumed schema of int(COS) chunks per key.
                # If there are orphans which are not matching the arcdatakeypattern they will
                # be computed as if they were COS.
                size = int(r.headers.get("X-Scal-Size",False)) + int(r.headers.get("X-Scal-Size",False))*int(COS)
            return ( key, r.status_code, size)
        else:
            # If the key is not found (HTTP code != 200) then return the key, the status code, and 0 for the size
            return ( key, r.status_code, 0)
    except requests.exceptions.ConnectionError as e:
        # If there is a connection error then return the key, the status code, and 0 for the size
        return ( key, "HTTP_ERROR", 0)


files = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
# Create a dataframe from the csv file not using the header, the columns will be _c0, _c1, _c2
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
# Create a resilient distributed dataset (RDD) from the dataframe (logical partitions of data)
# The rdd is a collection of tuples returned from statkey (key, status_code, size)
rdd = df.rdd.map(statkey)

#rdd1 = rdd.toDF()

# The size_computed is the sum of the size column in the rdd
size_computed= rdd.map(lambda x: (2,int(x[2]))).reduceByKey(lambda x,y: x + y).collect()[0][1]
string = "The total computed size of the not indexed keys is: %d bytes" % size_computed
banner = '\n' + '-' * len(string) + '\n'
print(banner + string + banner)

#totalsize = "file:///%s/output/s3fsck/output-size-computed-%s.csv" % (PATH, RING)
#rdd1.write.format("csv").mode("overwrite").options(header="false").save(totalsize)
