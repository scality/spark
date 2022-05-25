"""
count-stripes.py:count the total stripes per sparse file
output:%RING/sofs-files-COUNT-STRIPES.csv
"""
from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import sys
import yaml
import os

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]
PROTOCOL = cfg["protocol"]

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("count-stripes.py:Count the stripes::"+RING) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
     .config("spark.hadoop.fs.s3a.access.key", cfg["s3"]["access_key"])\
     .config("spark.hadoop.fs.s3a.secret.key", cfg["s3"]["secret_key"])\
     .config("spark.hadoop.fs.s3a.endpoint", cfg["s3"]["endpoint"])\
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


# single = "%s:///%s/output/output-sofs-files-%s.csv" % (PROT,PATH,RING)
single = "%s:///%s/%s/sofs-files.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(single)

dfneg = df.filter( df["subkey"].rlike(r"(REQUEST_TIMEOUT|empty|SCAL_*)"))
df = df.subtract(dfneg)
df = df.groupBy("key").count()

# single = "%s:///%s/output/output-sofs-files-COUNT-STRIPES-%s.csv" %  (PROT,PATH,RING)
single = "%s:///%s/%s/sofs-files-COUNT-STRIPES.csv" % (PROTOCOL, PATH, RING)
df.write.format('csv').mode("overwrite").options(header='true').save(single)
