"""
check_files_p4.py:Report the inode for bad sparse file
output:/%PATH/%RING/sofs-SPARSE-FILE-SHAPE.csv
"""

from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests
import yaml

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]
PROTOCOL = cfg["protocol"]


def hex_to_dec(row):
        key = row.key
        hex = key[6:22]
        dec = long(hex,16)
        return {'key':str(key),"hex":str(hex),"inode":str(dec)}

spark = SparkSession.builder \
     .appName("check_files_p4.py:Report the inode for bad sparse files:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()


inodes =   "%s:///%s/%s/inodes-*.txt" % (PROTOCOL, PATH, RING)
schema = StructType([
        StructField("inode", StringType(), False),
        StructField("path", StringType(), False)]
)
dfinodes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").schema(schema).load(inodes)

dfinodes.show(10,False)


# all = "file:///%s/output/output-sofs-SPARSE-FILE-SHAPE-%s.csv" % (PATH,RING)
all = "%s:///%s/%s/sofs-SPARSE-FILE-SHAPE.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(all)


df = df.filter(df["good_state"]== 'false' )
df = df.rdd.map(lambda x : hex_to_dec(x)).toDF()

df.show(10,False)

inner_join_true  =  dfinodes.join(df,["inode"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('inode','path','is_present')
inner_join_false =  dfinodes.join(df,["inode"], "leftanti").withColumn('is_present', F.lit(int(0))).select('inode','path','is_present')

df_final = inner_join_true.union(inner_join_false)

df_final = df_final.filter(df_final["is_present"]==1)
print df_final.show(10,False)

# all = "file:///%s/output/output-sofs-SPARSE-FILE-SHAPE-INODE-%s.csv" % (PATH,RING)
all = "%s:///%s/%s/sofs-SPARSE-FILE-SHAPE-INODE.csv" % (PROTOCOL, PATH, RING)
df.write.format('csv').mode("overwrite").options(header='false').save(all)

# all = "file:///%s/output/output-sofs-SPARSE-FILE-SHAPE-INODE-ALL-%s.csv" % (PATH,RING)
all = "%s:///%s/%s/sofs-SPARSE-FILE-SHAPE-INODE-ALL.csv" % (PROTOCOL, PATH, RING)
df_final.write.format('csv').mode("overwrite").options(header='false').save(all)
