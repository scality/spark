from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import sys
import yaml

config_path = "%s/%s" % ( sys.path[0] ,"./config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]


def hex_to_dec(row):
        key = row._c1
	hex = key[25:30]
	dec = int(key[25:30],16)
	return (key,hex,dec)

spark = SparkSession.builder \
     .appName("Count flags ring::"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


files = "file:///%s/listkeys-%s.csv" % (PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.filter( df["_c1"].rlike(r".*0500000040$") )
df = df.groupBy("_c1").count()
volume = df.rdd.map(hex_to_dec)
volumes = volume.toDF()

print volumes.show(100,False)
