"""Docstring"""

import os
import sys
import yaml
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

CONFIG_PATH = f"{sys.path[0]}/../config/config.yml"
with open(CONFIG_PATH, "r", encoding="utf-8") as ymlfile:
    CFG = yaml.load(ymlfile, Loader=yaml.SafeLoader)

RING = sys.argv[1] if len(sys.argv) > 1 else CFG["ring"]
PATH = CFG["path"]
PROTOCOL = CFG["protocol"]
ACCESS_KEY = CFG["s3"]["access_key"]
SECRET_KEY = CFG["s3"]["secret_key"]
ENDPOINT_URL = CFG["s3"]["endpoint"]
COS = CFG["cos_protection"]

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
SPARK = (
    SparkSession.builder.appName(f"s3_fsck_p1.py:Build RING keys :{RING}")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)
    .config("spark.executor.instances", CFG["spark.executor.instances"])
    .config("spark.executor.memory", CFG["spark.executor.memory"])
    .config("spark.executor.cores", CFG["spark.executor.cores"])
    .config("spark.driver.memory", CFG["spark.driver.memory"])
    .config("spark.memory.offHeap.enabled", CFG["spark.memory.offHeap.enabled"])
    .config("spark.memory.offHeap.size", CFG["spark.memory.offHeap.size"])
    .config("spark.local.dir", CFG["path"])
    .getOrCreate()
)

FILES = f"{PROTOCOL}://{PATH}/{RING}/listkeys.csv"
DATA_FRAME = (
    SPARK.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(FILES)
)

# list the ARC SPLIT main chunks
DF_SPLIT = DATA_FRAME.filter(
    DATA_FRAME["_c1"].rlike(r".*000000..50........$") & DATA_FRAME["_c3"].rlike("0")
).select("_c1")

DF_ARC_SINGLE = DF_SPLIT.filter(DATA_FRAME["_c1"].rlike(r".*70$"))
DF_ARC_SINGLE = DF_ARC_SINGLE.groupBy("_c1").count().filter("count > 3")
DF_ARC_SINGLE = DF_ARC_SINGLE.withColumn("ringkey", DF_ARC_SINGLE["_c1"])

DF_COS_SINGLE = DF_SPLIT.filter(DATA_FRAME["_c1"].rlike(f".*{str(COS)}0$"))
DF_COS_SINGLE = DF_COS_SINGLE.groupBy("_c1").count()
DF_COS_SINGLE = DF_COS_SINGLE.withColumn("ringkey", DF_COS_SINGLE["_c1"])
DF_COS_SINGLE = DF_COS_SINGLE.withColumn(
    "_c1", F.expr("substring(_c1, 1, length(_c1)-14)")
)

DF_ARC_SINGLE = DF_ARC_SINGLE.union(DF_COS_SINGLE)

# list the ARC KEYS
DF_SYNC = DATA_FRAME.filter(DATA_FRAME["_c1"].rlike(r".*000000..51........$")).select(
    "_c1"
)

DF_ARC_SYNC = DF_SYNC.filter(DATA_FRAME["_c1"].rlike(r".*70$"))
DF_ARC_SYNC = DF_ARC_SYNC.groupBy("_c1").count().filter("count > 3")
DF_ARC_SYNC = DF_ARC_SYNC.withColumn("ringkey", DF_ARC_SYNC["_c1"])
DF_ARC_SYNC = DF_ARC_SYNC.withColumn("_c1", F.expr("substring(_c1, 1, length(_c1)-14)"))

DF_COS_SYNC = DF_SYNC.filter(DATA_FRAME["_c1"].rlike(f".*{str(COS)}0$"))
DF_COS_SYNC = DF_COS_SYNC.groupBy("_c1").count()
DF_COS_SYNC = DF_COS_SYNC.withColumn("ringkey", DF_COS_SYNC["_c1"])
DF_COS_SYNC = DF_COS_SYNC.withColumn("_c1", F.expr("substring(_c1, 1, length(_c1)-14)"))

DF_ARC_SYNC = DF_ARC_SYNC.union(DF_COS_SYNC)

DF_TOTAL = DF_ARC_SYNC.union(DF_ARC_SINGLE)
TOTAL = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/arc-keys.csv"
DF_TOTAL.write.format("csv").mode("overwrite").options(header="true").save(TOTAL)
