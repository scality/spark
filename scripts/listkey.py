import os
import sys
import shutil
import requests
import time
requests.packages.urllib3.disable_warnings()

from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.key import Key
from scality.storelib.storeutils import uks_parse


import yaml

import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


config_path = "%s/%s" % ( sys.path[0] ,"config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

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
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'

def prepare_path():
    try:
        shutil.rmtree(PATH)
    except:
        pass
    if not os.path.exists(PATH):
        os.makedirs(PATH)

def listkeys(row, now):
    klist = []
    n = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(row.ip, row.adminport), chord_addr=row.ip, chord_port=row.chordport, dso=RING)
    params = { "mtime_min":"123456789", "mtime_max":now, "loadmetadata":"browse"}
    for k in n.listKeysIter(extra_params=params):
        if len(k.split(",")[0]) > 30 :
            klist.append([ k.rstrip().split(',')[i] for i in [0,1,2,3] ])
            # data = [ k.rstrip().split(',')[i] for i in [0,1,2,3] ]
            # data = ",".join(data)
            # print >> f, data
    print [( row.ip, row.adminport, 'OK')]
    return klist

now = int(str(time.time()).split('.')[0]) - RETENTION
prepare_path()
s = Supervisor(url=URL, login=USER, passwd=PASSWORD)
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])
df = spark.createDataFrame(listm)
print df.show(36, False)

dfnew = df.repartition(36)
dfklist = dfnew.rdd.map(lambda x:listkeys(x, now))
dfklist = dfklist.flatMap(lambda x: x).toDF()
dfklist.write.format("csv").mode("overwrite").options(header="true").save(files)
