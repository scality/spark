from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import re
import requests
import binascii
import hashlib
import base64
import yaml

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]

SREBUILDD_IP = cfg["srebuildd_ip"]
SREBUILDD_ARC_PATH = cfg["srebuildd_arc_path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
ARC = cfg["arc_protection"]
COS = cfg["cos_protection"]
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

# The arcindex is a map between the ARC Schema and the hex value found in the ringkey in the 24 bits preceding the last 8 bits of the key
arcindex = {"4+2": "102060", "8+4": "2040C0", "9+3": "2430C0", "7+5": "1C50C0", "5+7": "1470C0"}

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'

spark = SparkSession.builder \
     .appName("s3_fsck_p0.py:Translate the S3 ARC keys :" + RING) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
     .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
     .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
     .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()



def pad2(n):
    x = '%s' % (n,)
    return ('0' * (len(x) % 2)) + x

def to_bytes(h):
    return binascii.unhexlify(h)

def get_digest(name):
    m = hashlib.md5()
    m.update(name)
    digest = bytearray(m.digest())
    return digest

def get_dig_key(name):
    digest = get_digest(name)
    hash_str =  digest[0] << 16 |  digest[1] << 8  | digest[2]
    oid = digest[3] << 56 |  digest[4] << 48 |  \
        digest[5] << 40 | digest[6] << 32 |   \
        digest[7] << 24 |  digest[8] << 16  | digest[9] << 8 | digest[10]
    hash_str = "{0:x}".format(hash_str)
    oid = "{0:x}".format(oid)
    oid = oid.zfill(16)
    volid = "00000000"
    svcid = "51"
    specific = arcindex[ARC] #Make sure to set arc_protection in config when ARC schema changes
    cls = "70"
    key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
    return key.zfill(40)

def gen_md5_from_id(key):
    key = key.lstrip("0")
    key = pad2(key)
    int_b = to_bytes(key)
    return get_dig_key(int_b)


def sparse(f):
    lst  = []
    m = re.findall(r'(200000000000014|20100000014)([0-9-a-f]{40})', f)
    n = re.findall(r'(200000000000013|20100000013)([0-9-a-f]{38})', f)
    o = re.findall(r'(200000000000012|20100000012)([0-9-a-f]{36})', f)
    marc =  re.findall(r'(51d68800000014)([0-9-a-f]{40})', f)
    narc =  re.findall(r'(51d68800000013)([0-9-a-f]{38})', f)
    oarc =  re.findall(r'(51d68800000012)([0-9-a-f]{36})', f)
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
    """Check if the RING key is split or not. Return True if split, False if not split, None if error (422, 404, 50X, etc.)"""
    url = "http://%s:81/%s/%s" % (SREBUILDD_IP, SREBUILDD_ARC_PATH, str(key.zfill(40)))
    r = requests.head(url)
    if r.status_code == 200:
        return {"result": True, "is_split": r.headers.get("X-Scal-Attr-Is-Split", False)}
    return {"result": False, "is_split": None}


def blob(row):
    """Return a list of dict with the sproxyd input key, its subkey if it exists and digkey"""
    # set key from row._c2 (column 3) which contains an sproxyd input key
    key = row._c2
    # use the sproxyd input key to find out if the key is split or not
    # check_split(key) is used to transform the input key into a RING key, assess if it exists AND whether it is a SPLIT.
    split = check_split(key)
    if not split['result']:
        # If the key is not found, return a dict with the key, subkey and digkey set to NOK_HTTP
        return [{"key":key, "subkey":"NOK_HTTP", "digkey":"NOK_HTTP"}]
    if split['is_split']:
        try:
            header = { "x-scal-split-policy": "raw" }
            url = "http://%s:81/%s/%s" % (
                SREBUILDD_IP,
                SREBUILDD_ARC_PATH,
                str(key.zfill(40)),
            )
            response = requests.get(url, headers=header, stream=True)
            if response.status_code == 200:
                chunks = ""
                for chunk in response.iter_content(chunk_size=1024000000):
                    if chunk:
                        chunks = chunk + chunk
                chunkshex = chunks.encode("hex")
                rtlst = []
                # the k value is the subkey, a subkey is the sproxyd input key for each stripe of the split
                for k in list(set(sparse(chunkshex))):
                    # "key": key == primary sproxyd input key of a split object
                    # "subkey": k == subkey sproxyd input key of an individual stripe of a split object
                    # "digkey": gen_md5_from_id(k)[:26] == md5 of the subkey
                    # digkey: the unqiue part of a main chunk before service id,
                    # arc schema, and class are appended
                    rtlst.append(
                        {"key": key, "subkey": k, "digkey": gen_md5_from_id(k)[:26]}
                    )
                # If the key is split and request is OK:
                # return a list of dicts with the key (primary sproxyd input key),
                # subkey (sproxyd input key of a split stripe) and
                # digkey, (md5 of the subkey)
                # digkey: the unqiue part of a main chunk before service id,
                # arc schema, and class are appended
                return rtlst
            # If the key is split and request is not OK:
            # return a dict with the key (primary sproxyd input key)
            # with both subkey and digkey columns set to NOK
            return [{"key": key, "subkey": "NOK", "digkey": "NOK"}]
        except requests.exceptions.ConnectionError as e:
            # If there is a Connection Error in the HTTP request:
            # return a dict with the key(primary sproxyd input key),
            # with both subkey and digkey set to NOK
            return [{"key": key, "subkey": "NOK_HTTP", "digkey": "NOK_HTTP"}]
    if not split['is_split']:
        # If the key is not split:
        # return a dict with the key (primary sproxyd input key),
        # subkey set to SINGLE and
        # digkey, (md5 of the subkey)
        # digkey: the unqiue part of a main chunk before service id,
        # arc schema, and class are appended
        return [{"key": key, "subkey": "SINGLE", "digkey": gen_md5_from_id(key)[:26]}]

new_path = os.path.join(PATH, RING, "s3-bucketd")
files = "%s://%s" % (PROTOCOL, new_path)

# s3-bucketd structure: 
# { bucket, s3 object, sproxyd input key }
# e.g. test,2022%2F04%2F23%2Fclients%2F798a98d2367e8d5d4%2Fobject%2F1000.idx,08BC471F14C77A14C2F1A9C78F6BCD59FB7A5B20
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(files)

# repartition the dataframe to have the same number of partitions as the number of executors * cores
df = df.repartition(PARTITIONS)
rdd = df.rdd.map(lambda x : blob(x))
dfnew = rdd.flatMap(lambda x: x).toDF()

single = "%s://%s/%s/s3fsck/s3-dig-keys.csv" % (PROTOCOL, PATH, RING)
# write the dataframe to a csv file with a header
dfnew.write.format("csv").mode("overwrite").options(header="true").save(single)
