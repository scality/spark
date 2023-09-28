from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
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
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
COS = cfg["cos_protection"]

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p1.py:Build RING keys :" + RING) \
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


files = "%s://%s/%s/listkeys.csv" % (PROTOCOL, PATH, RING)
# reading without a header, the _c0, _c1, _c2, _c3 are the default column names for column 1, 2, 3, 4  
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(files)

# list the ARC SPLIT main chunks with service ID 50 from column 2
df_split = df.filter(df["_c1"].rlike(r".*000000..50........$") & df["_c3"].rlike("0")).select("_c1")

# Match keys which end in 70 from column 2, to a new dataframe dfARCsingle
dfARCsingle = df_split.filter(df["_c1"].rlike(r".*70$"))
# Filter out when less than 3 stripe chunks (RING orphans)
dfARCsingle = dfARCsingle.groupBy("_c1").count().filter("count > 3")

# rename dfARCsingle _c1 (column 2): it is now the "ringkey" (aka. "main chunk") ???
dfARCsingle = dfARCsingle.withColumn("ringkey",dfARCsingle["_c1"])

# in df_split, filter _c1 (column 2) RING key main chunk for specific COS protection
dfCOSsingle = df_split.filter(df["_c1"].rlike(r".*" + str(COS) + "0$"))

# count the number of chunks in _c1 (column 2) found for each key
dfCOSsingle = dfCOSsingle.groupBy("_c1").count()
# dfCOSsingle _c1 (column 2) is now the ringkey ???
dfCOSsingle = dfCOSsingle.withColumn("ringkey",dfCOSsingle["_c1"])
# ???
dfCOSsingle = dfCOSsingle.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# union the ARC and COS single chunks
dfARCsingle = dfARCsingle.union(dfCOSsingle)

# list the ARC KEYS with service ID 51
df_sync = df.filter(df["_c1"].rlike(r".*000000..51........$")).select("_c1")

# Match keys which end in 70 from column 2
dfARCSYNC = df_sync.filter(df["_c1"].rlike(r".*70$"))
# Filter out when less than 3 stripe chunks (RING orphans)
dfARCSYNC = dfARCSYNC.groupBy("_c1").count().filter("count > 3")
# dfARCSYNC _c1 (column 2) is now the ringkey ???
dfARCSYNC = dfARCSYNC.withColumn("ringkey",dfARCSYNC["_c1"])
# filter _c1 (column 2) for specific COS protection
dfARCSYNC = dfARCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# filter _c1 (column 2) for specific COS protection
dfCOCSYNC = df_sync.filter(df["_c1"].rlike(r".*" + str(COS) + "0$"))
# count the number of chunks in _c1 (column 2) found for each key
dfCOCSYNC = dfCOCSYNC.groupBy("_c1").count()
# dfCOCSYNC _c1 (column 2) is now the ringkey ???
dfCOCSYNC = dfCOCSYNC.withColumn("ringkey",dfCOCSYNC["_c1"])
# ???
dfCOCSYNC = dfCOCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# union the ARC and COS SYNC chunks
dfARCSYNC = dfARCSYNC.union(dfCOCSYNC)

# union the ARC and COS SYNC and single chunks to get the total list of keys
dftotal = dfARCSYNC.union(dfARCsingle)
total = "%s://%s/%s/s3fsck/arc-keys.csv" % (PROTOCOL, PATH, RING)
dftotal.write.format("csv").mode("overwrite").options(header="true").save(total)
