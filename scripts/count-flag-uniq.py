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

spark = SparkSession.builder \
     .appName("Count uniq flags ring::"+RING) \
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

print df.groupBy("_c3").agg(F.countDistinct("_c1")).show()
