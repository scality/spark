"""Docstring"""
import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml

CONFIG_PATH = f"{sys.path[0]}/../config/config.yml"
with open(CONFIG_PATH, "r", encoding="utf-8") as ymlfile:
    CFG = yaml.load(ymlfile, Loader=yaml.SafeLoader)

RING = sys.argv[1] if len(sys.argv) > 1 else CFG["ring"]
PATH = CFG["path"]
PROTOCOL = CFG["protocol"]
ACCESS_KEY = CFG["s3"]["access_key"]
SECRET_KEY = CFG["s3"]["secret_key"]
ENDPOINT_URL = CFG["s3"]["endpoint"]

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
SPARK = (
    SparkSession.builder.appName(
        f"s3_fsck_p2.py:Union the S3 keys and the RING keys :{RING}"
    )
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

S3_KEYS = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/s3-dig-keys.csv"
RING_KEYS = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/arc-keys.csv"

DATA_FRAME_S3_KEYS = (
    SPARK.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(S3_KEYS)
)
DATA_FRAME_RING_KEYS = (
    SPARK.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(RING_KEYS)
)

DATA_FRAME_RING_KEYS = DATA_FRAME_RING_KEYS.withColumnRenamed("_c1", "digkey")

INNER_JOIN_FALSE = (
    DATA_FRAME_RING_KEYS.join(DATA_FRAME_S3_KEYS, ["digkey"], "leftanti")
    .withColumn("is_present", F.lit(0))
    .select("ringkey", "is_present", "digkey")
)
DATA_FRAME_FINAL = INNER_JOIN_FALSE.select("ringkey")
ALL_MISSING = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/s3objects-missing.csv"
DATA_FRAME_FINAL.write.format("csv").mode("overwrite").options(header="false").save(
    ALL_MISSING
)
