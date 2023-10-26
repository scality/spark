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
# FIXME skip native column names, rename on the fly.
# reading without a header,
# columns _c0, _c1, _c2, _c3 are the default column names of 
# columns   1,   2,   3,   4 for the csv
# REQUIRED  N,   Y,   N,   Y
# input structure: (RING key, main chunk, disk, flag)
# e.g. 555555A4948FAA554034E155555555A61470C07A,8000004F3F3A54FFEADF8C00000000511470C070,g1disk1,0
# Required Fields:
#   - _c1 (main chunk)
#   - _c3 (flag)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(files)

# list the ARC_REPLICATED main chunks from column 2 with service ID 50 and flag = 0 (new), into a single column (vector) named "_c1" 
# FIXME rename column on the fly
df_split = df.filter(df["_c1"].rlike(r".*000000..50........$") & df["_c3"].rlike("0")).select("_c1")

# In df_split, match keys which end in 70 from column with litteral name "_c1", to a new dataframe dfARCsingle
# FIXME explain what we're trying to accomplish by identifying such keys (0x50: ARC_REPLICATED, per UKS --> calls for a COS ending, not an ARC ending). Under what circumstances should such keys exist?
dfARCsingle = df_split.filter(df["_c1"].rlike(r".*70$"))
# Filter out when strictly less than 4 stripe chunks (RING orphans), creating a new "count" column on the fly
# dfARCsingle now has two columns ("_c1", "count")
dfARCsingle = dfARCsingle.groupBy("_c1").count().filter("count > 3")

# in dfARCsingle, duplicate column named "_c1" into a new "ringkey" (aka. "main chunk") column 
# dfARCsingle now has three columns ("_c1", "count", "ringkey")
# FIXME do the renaming some place else, e.g. upon dataframe creation, be consistent about it
dfARCsingle = dfARCsingle.withColumn("ringkey",dfARCsingle["_c1"])

# in df_split (a vector of main chunks), filter column named "_c1" RING key main chunk for the configured COS protection
dfCOSsingle = df_split.filter(df["_c1"].rlike(r".*" + str(COS) + "0$"))

# count the number of chunks in column named "_c1" found for each key, creating the "count" column on the fly
# dfCOSsingle now has two columns ("_c1", "count")
dfCOSsingle = dfCOSsingle.groupBy("_c1").count()
# in dfCOSsingle, duplicate column named "_c1" into a new "ringkey" (aka. "main chunk") column 
# dfCOSsingle now has three columns ("_c1", "count", "ringkey")
dfCOSsingle = dfCOSsingle.withColumn("ringkey",dfCOSsingle["_c1"])
# in dfCOSsingle, do an in-place substring operation on column "_c1": get the 26 first characters of the main chunk (MD5 hash of the input key + 4 extra chars)
# FIXME: say why we need those 4 extra characters (about 18% more weight than the 22-char md5 alone)
dfCOSsingle = dfCOSsingle.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# union the dfCOSsingle and dfARCsingle dataframes ("_c1", "count", "ringkey")
dfARCsingle = dfARCsingle.union(dfCOSsingle)

# list the ARC_SINGLE keys with service ID 51
# repeat the same logic as before, with a different initial mask
# Output is a three-column matrix that will be unioned with the previous dataframe dfARCsingle
df_sync = df.filter(df["_c1"].rlike(r".*000000..51........$")).select("_c1")

# Match keys which end in 70 from column 2
dfARCSYNC = df_sync.filter(df["_c1"].rlike(r".*70$"))
# Filter out when less than 3 stripe chunks (RING orphans)
dfARCSYNC = dfARCSYNC.groupBy("_c1").count().filter("count > 3")
# dfARCSYNC "_c1" column is duplicated into a "ringkey" column
dfARCSYNC = dfARCSYNC.withColumn("ringkey",dfARCSYNC["_c1"])
# in dfARCSYNC, do an in-place substring operation on column "_c1": get the 26 first characters of the main chunk (MD5 hash of the input key + 4 extra chars)
dfARCSYNC = dfARCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# filter "_c1" for configured COS protection
dfCOCSYNC = df_sync.filter(df["_c1"].rlike(r".*" + str(COS) + "0$"))
# count the number of chunks in "_c1" found for each key
dfCOCSYNC = dfCOCSYNC.groupBy("_c1").count()
# dfCOCSYNC "_c1" column is duplicated into a "ringkey" column
dfCOCSYNC = dfCOCSYNC.withColumn("ringkey",dfCOCSYNC["_c1"])
# in dfCOCSYNC, do an in-place substring operation on column "_c1": get the 26 first characters of the main chunk (MD5 hash of the input key + 4 extra chars)
dfCOCSYNC = dfCOCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

# union the two previous dataframes
dfARCSYNC = dfARCSYNC.union(dfCOCSYNC)

# union again the two outstanding dataframes dfARCSYNC and dfARCSINGLE into a dftotal dataframe
dftotal = dfARCSYNC.union(dfARCsingle)
total = "%s://%s/%s/s3fsck/arc-keys.csv" % (PROTOCOL, PATH, RING)
dftotal.write.format("csv").mode("overwrite").options(header="true").save(total)
