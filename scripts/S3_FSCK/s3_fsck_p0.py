"""Translate the S3 ARC keys to md5 keys"""
import binascii
import hashlib
import os
import sys
import re
import yaml
from pyspark.sql import SparkSession
import requests

CONFIG_PATH = f"{sys.path[0]}/../config/config.yml"
with open(CONFIG_PATH, "r", encoding="utf-8") as ymlfile:
    CFG = yaml.load(ymlfile, Loader=yaml.SafeLoader)

RING = sys.argv[1] if len(sys.argv) > 1 else CFG["ring"]
PATH = CFG["path"]

SREBUILDD_IP = CFG["srebuildd_ip"]
SREBUILDD_ARC_PATH = CFG["srebuildd_arc_path"]
PROTOCOL = CFG["protocol"]
ACCESS_KEY = CFG["s3"]["access_key"]
SECRET_KEY = CFG["s3"]["secret_key"]
ENDPOINT_URL = CFG["s3"]["endpoint"]
ARC = CFG["arc_protection"]
COS = CFG["cos_protection"]
PARTITIONS = int(CFG["spark.executor.instances"]) * int(CFG["spark.executor.cores"])

ARCINDEX = {
    "4+2": "102060",
    "8+4": "2040C0",
    "9+3": "2430C0",
    "7+5": "1C50C0",
    "5+7": "1470C0",
}

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'

SPARK = (
    SparkSession.builder.appName(
        f"s3_fsck_p0.py:Translate the S3 ARC keys :{RING}"
    )
    .config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)
    .config("spark.executor.instances", CFG["spark.executor.instances"])
    .config("spark.executor.memory", CFG["spark.executor.memory"])
    .config("spark.executor.cores", CFG["spark.executor.cores"])
    .config("spark.driver.memory", CFG["spark.driver.memory"])
    .config(
        "spark.memory.offHeap.enabled", CFG["spark.memory.offHeap.enabled"]
    )
    .config("spark.memory.offHeap.size", CFG["spark.memory.offHeap.size"])
    .config("spark.local.dir", CFG["path"])
    .getOrCreate()
)


def pad2(n):
    """Pad a string with 0s to make it even length"""
    x = f"{n}"
    return ("0" * (len(x) % 2)) + x


def to_bytes(h):
    """Convert a hex string to bytes"""
    return binascii.unhexlify(h)


def get_digest(name):
    """Get the md5 digest of a string"""
    m = hashlib.md5()
    m.update(name)
    digest = bytearray(m.digest())
    return digest


def get_dig_key(name):
    """Get the md5 digest of a string"""
    digest = get_digest(name)
    hash_str = digest[0] << 16 | digest[1] << 8 | digest[2]
    oid = (
        digest[3] << 56
        | digest[4] << 48
        | digest[5] << 40
        | digest[6] << 32
        | digest[7] << 24
        | digest[8] << 16
        | digest[9] << 8
        | digest[10]
    )
    hash_str = "{0:x}".format(hash_str)
    oid = "{0:x}".format(oid)
    oid = oid.zfill(16)
    volid = "00000000"
    svcid = "51"
    # Make sure to set arc_protection in config when ARC schema changes
    specific = ARCINDEX[ARC]
    cls = "70"
    key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
    return key.zfill(40)


def gen_md5_from_id(key):
    """Generate a md5 key from an id"""
    key = key.lstrip("0")
    key = pad2(key)
    int_b = to_bytes(key)
    return get_dig_key(int_b)


def sparse(chunk_hex):
    """Get the sparse keys from a file"""
    lst = []
    m = re.findall(r"(200000000000014|20100000014)([0-9-a-f]{40})", chunk_hex)
    n = re.findall(r"(200000000000013|20100000013)([0-9-a-f]{38})", chunk_hex)
    o = re.findall(r"(200000000000012|20100000012)([0-9-a-f]{36})", chunk_hex)
    marc = re.findall(r"(51d68800000014)([0-9-a-f]{40})", chunk_hex)
    narc = re.findall(r"(51d68800000013)([0-9-a-f]{38})", chunk_hex)
    oarc = re.findall(r"(51d68800000012)([0-9-a-f]{36})", chunk_hex)
    for mm in m:
        key = mm[1]
        lst.append(key.upper())
    for nn in n:
        key = nn[1]
        lst.append(key.upper())
    for oo in o:
        key = oo[1]
        lst.append(key.upper())
    for mmarc in marc:
        key = mmarc[1]
        lst.append(key.upper())
    for nnarc in narc:
        key = nnarc[1]
        lst.append(key.upper())
    for ooarc in oarc:
        key = oarc[1]
        lst.append(key.upper())
    return lst


def check_split(key):
    """Check if a key is split"""
    url = f"http://{SREBUILDD_IP}:81/{SREBUILDD_ARC_PATH}/{str(key.zfill(40))}"
    response = requests.head(url, timeout=3)
    if response.status_code == 200:
        return response.headers.get("X-Scal-Attr-Is-Split", False)
    return "HTTP_NOK"


def blob(row):
    """Get the blob from a key"""
    # pylint: disable=protected-access
    key = row._c2
    split = check_split(key)
    if split:
        try:
            header = {}
            header["x-scal-split-policy"] = "raw"
            url = f"http://{SREBUILDD_IP}:81/{SREBUILDD_ARC_PATH}/{str(key.zfill(40))}"
            response = requests.get(url, headers=header, stream=True, timeout=3)
            if response.status_code == 200:
                chunks = ""
                for chunk in response.iter_content(chunk_size=1_024_000_000):
                    if chunk:
                        chunks = chunk + chunk

                chunkshex = chunks.encode("hex")
                rtlst = []
                for subkey in list(set(sparse(chunkshex))):
                    rtlst.append(
                        {"key": key, "subkey": subkey, "digkey": gen_md5_from_id(subkey)[:26]}
                    )
                return rtlst
            return [{"key": key, "subkey": "NOK", "digkey": "NOK"}]

        except requests.exceptions.ConnectionError:
            return [{"key": key, "subkey": "NOK_HTTP", "digkey": "NOK_HTTP"}]
    elif split is False:
        return [{"key": key, "subkey": "SINGLE", "digkey": gen_md5_from_id(key)[:26]}]


NEW_PATH = os.path.join(PATH, RING, "s3-bucketd")
FILES = f"{PROTOCOL}://{NEW_PATH}"

DATA_FRAME = (
    SPARK.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(FILES)
)

DATA_FRAME = DATA_FRAME.repartition(PARTITIONS)
RD_DATASET = DATA_FRAME.rdd.map(lambda x: blob(x))
DATA_FRAME_NEW = RD_DATASET.flatMap(lambda x: x).toDF()

SINGLE = f"{PROTOCOL}://{PATH}/{RING}/s3fsck/s3-dig-keys.csv"
DATA_FRAME_NEW.write.format("csv").mode("overwrite").options(header="true").save(SINGLE)
