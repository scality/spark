"""
check_files_p2.py: Translate (dig) the Stripes keys
output:output/output-sofs-files-DIG-%RING.csv
"""

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


spark = SparkSession.builder \
     .appName("check_files_p2.py:Translate (dig) the Stripes keys:"+RING) \
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
      specific = "144090"
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      return key.zfill(40)

def gen_md5_from_id(key):
     try:
	key = key.lstrip('0')
        key = pad2(key)
	int_b = to_bytes(key)
	return get_dig_key(int_b)[:-14]
     except Exception  as e:
	print "erro_"+str(e)

def dig(row):
	key = row.key
	subkey = row.subkey
	arckey = gen_md5_from_id(subkey)
	return [{"key":key,"subkey":subkey ,"arckey": arckey}]


files = "file:///%s/output/output-sofs-files-%s.csv" % (PATH,RING)
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(files)

dfneg = df.filter( df["subkey"].rlike(r"(REQUEST_TIMEOUT|empty|SCAL_*)"))
df = df.subtract(dfneg)

rdd = df.rdd.map(lambda x : dig(x))
dfnew = rdd.flatMap(lambda x: x).toDF()

single = "file:///%s/output/output-sofs-files-DIG-%s.csv" % (PATH,RING)
dfnew.write.format('csv').mode("overwrite").options(header='true').save(single)

