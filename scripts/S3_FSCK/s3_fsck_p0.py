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
srebuildd_ip = cfg["srebuildd_ip"]

spark = SparkSession.builder \
     .appName("s3_fsck_p0.py:Translate the S3 ARC keys :"+RING) \
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
      specific = "102060" #Make sure to change it when the ARC schema changes
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      return key.zfill(40)

def gen_md5_from_id(key):
	key = key.lstrip('0')
        key = pad2(key)
        int_b = to_bytes(key)
        return get_dig_key(int_b)


def sparse(f):
        lst  = []
        m = re.findall(r'(200000000000014|20100000014)([0-9-a-f]{40})',f)
        n = re.findall(r'(200000000000013|20100000013)([0-9-a-f]{38})',f)
        o = re.findall(r'(200000000000012|20100000012)([0-9-a-f]{36})',f)
        marc =  re.findall(r'(51d68800000014)([0-9-a-f]{40})',f)
        narc =  re.findall(r'(51d68800000013)([0-9-a-f]{38})',f)
        oarc =  re.findall(r'(51d68800000012)([0-9-a-f]{36})',f)
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

	url = "http://%s:81/rebuild/arc/%s" % (srebuildd_ip, str(key.zfill(40)))
	r = requests.head(url)
	if r.status_code == 200:
		split = r.headers.get('X-Scal-Attr-Is-Split',False)
		return split
	else:
		return ("HTTP_NOK")

def blob(row):
	key = row._c2
	split = check_split(key)
	if split:
		try:
			header = {}
			header['x-scal-split-policy'] = "raw"
			url = "http://%s:81/rebuild/arc/%s" % (srebuildd_ip, str(key.zfill(40)))
			r = requests.get(url,headers=header,stream=True)
			if r.status_code == 200:
				chunks = ""
				for chunk in r.iter_content(chunk_size=1024000000):
					if chunk:
						chunks=chunk+chunk

				chunkshex =  chunks.encode('hex')
				rtlst = []
				for k in list(set(sparse(chunkshex))):
					rtlst.append({"key":key,"subkey":k,"digkey":gen_md5_from_id(k)[:26]})
				return rtlst
			else:
				return [{"key":key,"subkey":"NOK","digkey":"NOK"}]

		except requests.exceptions.ConnectionError as e:
			return [{"key":key,"subkey":"NOK_HTTP","digkey":"NOK_HTTP"}]
	elif split == False:
		return [{"key":key,"subkey":"SINGLE","digkey":gen_md5_from_id(key)[:26]}]
	

files = "file:///%s/s3-%s/" % (PATH,RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter", ";").load(files)

df = df.repartition(4)
rdd = df.rdd.map(lambda x : blob(x))
dfnew = rdd.flatMap(lambda x: x).toDF()

single = "file:///%s/output/s3fsck/s3-dig-keys-%s.csv" % (PATH,RING)
dfnew.write.format('csv').mode("overwrite").options(header='true').save(single)
