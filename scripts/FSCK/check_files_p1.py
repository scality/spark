
"""
check_files_p1.py: Check using the sparse micro-service the keys.
output:output/output-sofs-files-%RING.csv
"""
from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkContext
import sys
import yaml
import requests
import json

config_path = "%s/%s" % ( sys.path[0] ,"../config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
	RING = sys.argv[1]
else:
	RING = cfg["ring"]

PATH = cfg["path"]


spark = SparkSession.builder \
     .appName("check_files_p1.py:Return the stripe keys:"+RING) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

def hex_to_dec(row):
        key = row._c1
	hex = key[6:22]
	dec = long(hex,16)
	return {'key':str(key),"hex":str(hex),"dec":str(dec)}


def blob(row):
	mainkey = row.key
	key = row.hex
	try:
		try:
			req_s = requests.Session()
			r = req_s.get('http://127.0.0.1:9999/sparse/'+str(key),timeout=6000)
			#r = requests.get('http://127.0.0.1:9999/sparse/'+str(key),timeout=600)
			if r.status_code == 200:
				rtlst = []
				payload = json.loads(r.text)
				if "SCAL" in payload[0] or "empty" in payload[0]:
					rtlst.append({"key":mainkey,"subkey":payload[0]})
					return rtlst
				for k in payload:
					rtlst.append({"key":mainkey,"subkey":k.zfill(40)})
				return rtlst
			else:
				return [{"key":mainkey,"subkey":"KO"}]
		except requests.exceptions.Timeout:
			return [{"key":mainkey,"subkey":"REQUEST_TIMEOUT"}]
		except requests.exceptions.RequestException as e:
			return [{"key":mainkey,"subkey":"REQUEST_ERROR"}]

	except Exception as e:
		return [{"key":mainkey,"subkey":"KO"}]

files = "file:///%s/listkeys-%s.csv" % (PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.filter( df["_c1"].rlike(r".*0801000040$") )
df = df.groupBy("_c1").count()

sparse = df.rdd.map(hex_to_dec)
schema = StructType([
 	StructField("dec", StringType(), False),
 	StructField("key", StringType(), False),
 	StructField("hex", StringType(), False)]
)
#sparse = sparse.toDF(schema)
sparse = sparse.toDF()

sparse_subkey = sparse.rdd.map(lambda x : blob(x))
sparse_subkey = sparse_subkey.flatMap(lambda x: x).toDF()

single = "file:///%s/output/output-sofs-files-%s.csv" % (PATH , RING)
sparse_subkey.write.format('csv').mode("overwrite").options(header='true').save(single)
