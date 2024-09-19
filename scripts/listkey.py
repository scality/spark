"""
WARNING: does not work on python 3.9+ due to a change in elementtree used in supervisor.py
This script is used to generate a list of keys in a ring.

Step 1. The driver reads the configuration from config.yml, sets the variables and get the list of nodes from the supervisor.
Step 2. The driver creates a Spark session and reads the list of nodes into a DataFrame to store the nodes's keys into a CSV file.
"""

import os
import sys
import shutil
import requests
import time
import yaml
import ssl
from pyspark.sql import SparkSession
from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory

requests.packages.urllib3.disable_warnings()

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


config_path = "%s/%s" % ( os.getcwd(),"config/config.yml")
print(f"config_path: {config_path}")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

if len(sys.argv) >1:
        RING = sys.argv[1]
else:
        RING = cfg["ring"]

# CLI and Config derived arguments using CAPITALS
USER = cfg["sup"]["login"]
PASSWORD = cfg["sup"]["password"]
URL = cfg["sup"]["url"]
CPATH = cfg["path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
RETENTION = cfg.get("retention", 604800)
PATH = "%s/%s/listkeys.csv" % (CPATH, RING)
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

files = "%s://%s/%s/listkeys.csv" % (PROTOCOL, CPATH, RING)

spark = SparkSession.builder.appName("Generate Listkeys ring:" + RING) \
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

# s3 = s3fs.S3FileSystem(anon=False, key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={'endpoint_url': ENDPOINT_URL})
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:3.5.2" pyspark-shell'

def prepare_path():
    try:
        shutil.rmtree(PATH)
    except Exception as e:
        print(f"An error occurred: {e}")
        pass
    if not os.path.exists(PATH):
        os.makedirs(PATH)

def listkeys(row, now):
    klist = []
    try:
        n = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url=f'https://{row.ip}:{row.adminport}', chord_addr=row.ip, chord_port=row.chordport, dso=RING)
        params = { "mtime_min":"123456789", "mtime_max":now, "loadmetadata":"browse"}
        for k in n.listKeysIter(extra_params=params):
            if len(k.split(",")[0]) > 30 :
                klist.append([ k.rstrip().split(',')[i] for i in [0,1,2,3] ])
        print(f"Node {row.ip}:{row.adminport} - keys found: {len(klist)}")
    except Exception as e:
        print(f"Error processing node {row.ip}:{row.adminport}: {e}")
    return klist

now = int(str(time.time()).split('.')[0]) - RETENTION
prepare_path()
s = Supervisor(url=URL, login=USER, passwd=PASSWORD)

config_dict = s.supervisorConfigDso(dsoname=RING)

WantedKeys = ["name", "ip", "chordport", "adminport"]
config_dict = [ { key:node[key] for key in WantedKeys } for node in config_dict['nodes'] ]

listm = sorted(config_dict, key=lambda x: x['name'])

df = spark.createDataFrame(listm)
print(f"Number of nodes: {df.count()}")

###
# You may want to force the number of partitions to the number of nodes (Optional).
###
# df = df.repartition(int(df.count()))
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# print(df.explain(True)) # Debug line to check the execution plan
print(df.show(50, False))

###
# The code above is executed on the "driver" only
# Starting this point, the following code is executed on the "workers"
###

dfklist = df.rdd.map(lambda x:listkeys(x, now))
print("Keys collected:", dfklist.collect())  # Debugging line to check if any keys are collected, It can happen if RETENTION is too high for example.

dfklist = dfklist.flatMap(lambda x: x).toDF()
dfklist.write.format("csv").mode("overwrite").options(header="true").save(files)
