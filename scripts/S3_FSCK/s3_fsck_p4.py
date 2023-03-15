"""Delete the extra keys from the ring"""
import os
import re
import sys
import requests
import yaml
from pyspark.sql import SparkSession

CONFIG_PATH = f"{sys.path[0]}/../config/config.yml"
with open(CONFIG_PATH, "r") as ymlfile:
    CFG = yaml.load(ymlfile)

RING = sys.argv[1] if len(sys.argv) > 1 else CFG["ring"]
USER = CFG["sup"]["login"]
PASSWORD = CFG["sup"]["password"]
URL = CFG["sup"]["url"]
PATH = CFG["path"]
SREBUILDD_IP = CFG["srebuildd_ip"]
SREBUILDD_PATH = CFG["srebuildd_chord_path"]
SREBUILDD_URL = f"http://{SREBUILDD_IP}:81/{SREBUILDD_PATH}"
PROTOCOL = CFG["protocol"]
ACCESS_KEY = CFG["s3"]["access_key"]
SECRET_KEY = CFG["s3"]["secret_key"]
ENDPOINT_URL = CFG["s3"]["endpoint"]
PARTITIONS = int(CFG["spark.executor.instances"]) * int(CFG["spark.executor.cores"])
ARC = CFG["arc_protection"]

ARC_INDEX = {
    "4+2": "102060",
    "8+4": "2040C0",
    "9+3": "2430C0",
    "7+5": "1C50C0",
    "5+7": "1470C0",
}
ARC_KEY_PATTERN = re.compile(r"[0-9a-fA-F]{38}70")

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
SPARK = (
    SparkSession.builder.appName(f"s3_fsck_p4.py:Clean the extra keys :{RING}")
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
    .config("spark.local.dir", PATH)
    .getOrCreate()
)


def deletekey(row):
    """Delete the key from the ring"""
    key = row.ringkey
    try:
        url = f"{SREBUILDD_URL}/{str(key.zfill(40))}"
        print(url)
        response = requests.delete(url)
        status_code = response.status_code
        # status_code = "OK"
        return (key, status_code, url)
    except requests.exceptions.ConnectionError:
        return (key, "ERROR_HTTP", url)


FILES = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/s3objects-missing.csv"
DATA_FRAME = (
    SPARK.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .load(FILES)
)
DATA_FRAME = DATA_FRAME.withColumnRenamed("_c0", "ringkey")
DATA_FRAME = DATA_FRAME.repartition(PARTITIONS)
RD_DATASET = DATA_FRAME.rdd.map(deletekey).toDF()
DELETED_ORPHANS = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/deleted-s3-orphans.csv"
RD_DATASET.write.format("csv").mode("overwrite").options(header="false").save(
    DELETED_ORPHANS
)
