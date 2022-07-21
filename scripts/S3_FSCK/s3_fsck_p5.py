#!/usr/bin/python2.7
'''
Read keys from stdin and tries to find them by running listKeys on their node.
'''

import sys, os, getopt , re

import subprocess
import psutil

import yaml
from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.key import Key
from scality.common import ScalDaemonExceptionCommandError
from scality.storelib.storeutils import uks_parse

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext


config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)


USER = cfg["sup"]["login"]
PASSWORD = cfg["sup"]["password"]
URL = cfg["sup"]["url"]
PATH = cfg["path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

if len(sys.argv) > 1:
    RING = sys.argv[ 1 ]
else:
    RING = cfg[ "ring" ]
sup = 'http://10.9.31.198:5580'

if not RING:
    sys.exit(2)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p5.py:Recover the keys :" + RING) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
     .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
     .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)\
     .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", PATH) \
     .getOrCreate()

s = Supervisor(url=sup,login=USER,passwd=PASSWORD)
nodes = {}
success = True
node = None
arck = None


def undeletekey(row):
    for connection in connections:
      if connection[5] == 'LISTEN':
        local = connection[3]
        if local[1] == 4244:
          nodeIP = local[0]

    ret = False
    key = Key(row.ringkey)
    sarc = [ key ] + [ k for k in key.getReplicas() ]
    print(sarc)
    localnode = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(nodeIP, "6444"), chord_addr=nodeIP, chord_port="4244", dso=RING)
    for arck in sarc:
        print("UNDELETE", arck.getHexPadded())
        p = localnode.findSuccessor(arck.getHexPadded())[ "address" ].split(":")
        node = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(
            p[ 0 ], str(int(p[ 1 ]) + 2200)), chord_addr=p[ 0 ], chord_port=p[
            1 ], dso=RING)
        try:
            et = node.chunkapiStoreOp(op="undelete", key=arck.getHexPadded(), extra_params={
                "version": 10000})
            print {"status": et.find("result").find("status").text}
            ret = True
        except Exception as e:
            ret = False
    if ret:
        return (key.getHexPadded(), "OK")
    else:
        return (key.getHexPadded(), "KO")


files = "%s://%s/%s/s3fsck/recover.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.withColumnRenamed("_c0", "ringkey")
df = df.repartition(PARTITIONS)
rdd = df.rdd.map(undeletekey).toDF()
recoveredorphans = "%s://%s/%s/s3fsck/recovered-ring-keys.csv" % (PROTOCOL, PATH, RING)
rdd.write.format("csv").mode("overwrite").options(header="false").save(recoveredorphans)
