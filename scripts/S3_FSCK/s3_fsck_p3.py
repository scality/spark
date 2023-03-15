"""Compute the total sizes to be deleted"""
import os
import re
import sys
import yaml
from pyspark.sql import SparkSession
import requests

CONFIG_PATH = f"{sys.path[0]}/../config/config.yml"
with open(CONFIG_PATH, "r", encoding=yaml.SafeLoader) as ymlfile:
    CFG = yaml.load(ymlfile)


RING = sys.argv[1] if len(sys.argv) > 1 else CFG["ring"]
PATH = CFG["path"]
PROTOCOL = CFG["protocol"]
ACCESS_KEY = CFG["s3"]["access_key"]
SECRET_KEY = CFG["s3"]["secret_key"]
ENDPOINT_URL = CFG["s3"]["endpoint"]
SREBUILDD_IP = CFG["srebuildd_ip"]
SREBUILDD_ARCDATA_PATH = CFG["srebuildd_arcdata_path"]
SREBUILDD_URL = f"http://{SREBUILDD_IP}:81/{SREBUILDD_ARCDATA_PATH}"
ARC = CFG["arc_protection"]
COS = CFG["cos_protection"]
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
    SparkSession.builder.appName(
        f"s3_fsck_p3.py:Compute the total sizes to be deleted :{RING}"
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
    .config("spark.local.dir", PATH)
    .getOrCreate()
)


def statkey(row):
    """Get the size of the key"""
    # pylint: disable=protected-access
    key = row._c0
    try:
        url = f"{SREBUILDD_URL}/{str(key.zfill(40))}"
        response = requests.head(url)
        if response.status_code == 200:
            if re.search(ARC_KEY_PATTERN, key):
                size = int(response.headers.get("X-Scal-Size", False)) * 12
            else:
                size = int(response.headers.get("X-Scal-Size", False)) + int(
                    response.headers.get("X-Scal-Size", False)
                ) * int(COS)
            return (key, response.status_code, size)
        return (key, response.status_code, 0)
    except requests.exceptions.ConnectionError:
        return (key, "HTTP_ERROR", 0)


FILES = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/s3objects-missing.csv"
DATA_FRAME = (
    SPARK.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .load(FILES)
)
RD_DATASET = DATA_FRAME.rdd.map(statkey)

SIZE_COMPUTED = (
    RD_DATASET.map(lambda x: (2, int(x[2])))
    .reduceByKey(lambda x, y: x + y)
    .collect()[0][1]
)
STRING = f"The total computed size of the not indexed keys is: {SIZE_COMPUTED} bytes"
BANNER = "\n" + "-" * len(STRING) + "\n"
print(BANNER + STRING + BANNER)

# totalsize = "file:///%s/output/s3fsck/output-size-computed-%s.csv" % (PATH, RING)
# rdd1.write.format("csv").mode("overwrite").options(header="false").save(totalsize)
