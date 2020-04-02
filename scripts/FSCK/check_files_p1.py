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

spark = SparkSession.builder.appName("Check Split Objects P1").getOrCreate()


RING = "IT"

RING = sys.argv[1]

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
      specific = "102060" #Make sure to change it when the ARC schema changes
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      return key.zfill(40)

def gen_md5_from_id(key):
	if key == "empty":
		return "empty"
	elif "err" in key:
		return key
	else:
		try:
			int_b = to_bytes(key)
			return get_dig_key(int_b)[:14]
		except Exception  as e:
			print "erro_"+str(e)


def dig(row):
	print row
	key = row.key
	subkey = row.subkey
	arckey = gen_md5_from_id(subkey)
	print arckey
	return [{"key":key,"subkey":subkey ,"arckey": arckey}]


files = "file:///fs/spark/output/output-sofs-files-%s.csv" % RING
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(files)

df.show(10,False)
rdd = df.rdd.map(lambda x : dig(x))
dfnew = rdd.flatMap(lambda x: x).toDF()
print dfnew.show(20,False)

single = "file:///fs/spark/output/output-sofs-files-ARC-%s.csv" % RING
dfnew.write.format('csv').mode("overwrite").options(header='true').save(single)

