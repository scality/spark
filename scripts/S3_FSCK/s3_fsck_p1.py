from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import yaml

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
PROT = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p1.py:Build RING keys :"+RING) \
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


files = "%s://%s/listkeys-%s.csv" % (PROT, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(files)

#list the ARC SPLIT main chunks
df_split = df.filter(df["_c1"].rlike(r".*000000..50........$") & df["_c3"].rlike("0")).select("_c1")

dfARCsingle = df_split.filter(df["_c1"].rlike(r".*70$"))
dfARCsingle = dfARCsingle.groupBy("_c1").count().filter("count > 3")
dfARCsingle = dfARCsingle.withColumn("ringkey",dfARCsingle["_c1"])

dfCOSsingle = df_split.filter(df["_c1"].rlike(r".*20$"))
dfCOSsingle = dfCOSsingle.groupBy("_c1").count()
dfCOSsingle = dfCOSsingle.withColumn("ringkey",dfCOSsingle["_c1"])
dfCOSsingle = dfCOSsingle.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

dfARCsingle = dfARCsingle.union(dfCOSsingle)

#list the ARC SYNC KEYS
df_sync = df.filter(df["_c1"].rlike(r".*000000..51........$") & df["_c3"].rlike("16")).select("_c1")

dfARCSYNC = df_sync.filter(df["_c1"].rlike(r".*70$"))
dfARCSYNC = dfARCSYNC.groupBy("_c1").count().filter("count > 3")
dfARCSYNC = dfARCSYNC.withColumn("ringkey",dfARCSYNC["_c1"])
dfARCSYNC = dfARCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

dfCOCSYNC = df_sync.filter(df["_c1"].rlike(r".*20$"))
dfCOCSYNC = dfCOCSYNC.groupBy("_c1").count()
dfCOCSYNC = dfCOCSYNC.withColumn("ringkey",dfCOCSYNC["_c1"])
dfCOCSYNC = dfCOCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

dfARCSYNC = dfARCSYNC.union(dfCOCSYNC)

dftotal = dfARCSYNC.union(dfARCsingle)
total = "%s://%s/output/s3fsck/input-arc-%s-keys.csv" % (PROT, PATH, RING)
dftotal.write.format("csv").mode("overwrite").options(header="true").save(total)
