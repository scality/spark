import os
import sys
import shutil
import requests
import time
requests.packages.urllib3.disable_warnings()
import re
import s3fs
import struct
import base64

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


config_path = "%s/%s" % ( sys.path[0], "config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

if len(sys.argv) >1:
        RING = sys.argv[1]
else:
        RING = cfg["ring"]

USER = cfg["sup"]["login"]
PASSWORD = cfg["sup"]["password"]
URL = cfg["sup"]["url"]
CPATH = cfg["path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
RETENTION = cfg.get("retention", 604800)
PATH = "%s/listkeys-%#s.csv" % (CPATH, RING)
PATH2 = "%s/%s/listkeys.csv" % (CPATH, RING)
PROTECTION = cfg["arc_protection"]

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

s3 = s3fs.S3FileSystem(anon=False, key=ACCESS_KEY, secret=SECRET_KEY, client_kwargs={'endpoint_url': ENDPOINT_URL})
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'

arcindex = {"4+2": "102060", "8+4": "12040C", "9+3": "2430C0", "7+5": "1C50C0", "5+7": "1470C0"}
arcdatakeypattern = re.compile(r'[0-9a-fA-F]{31}' + arcindex[PROTECTION] + '070')


def prepare_path():
    try:
        shutil.rmtree(PATH)
    except:
        pass
    if not os.path.exists(PATH):
        os.makedirs(PATH)



def revlookupid(key, node):
    if re.search(arcdatakeypattern, key):
        stat = node.chunkapiStoreOp(op='stat', key=key, dso=RING, extra_params={'use_base64': '1'})
        for s in stat.findall("result"):
            status = s.find("status").text
            if status == "CHUNKAPI_STATUS_OK":
                usermd = s.find("usermd").text
                if usermd is not None:
                    use_base64 = False
                    try:
                        use_base64 = s.find("use_base64").text
                        if int(use_base64) == 1:
                            use_base64 = True
                    except:
                        pass
                    if use_base64 is True:
                        rawusermd = base64.b64decode(usermd)
                        objectkeyinbytes = struct.unpack(">BBBBBBBBBBBBBBBBBBBB", rawusermd[21:41])
                        objectkeylist = []
                        for x in objectkeyinbytes:
                            raw = '{:02X}'.format(x)
                            objectkeylist.append(raw)
                        objectkey = ''.join(objectkeylist)
                    return status, objectkey
                else:
                    return "ERROR: No usermd found.", None
                        # data.append(str(objectkey))
            else:
                return status, None


def listkeys(row, now):
    # klist = []
    n = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(row.ip, row.adminport), chord_addr=row.ip, chord_port=row.chordport, dso=RING)
    fname = "%s/node-%s-%s.csv" % (PATH, row.ip, row.chordport)
    fname2 = "%s/node-%s-%s.csv" % (PATH2, row.ip, row.chordport)
    if PROTOCOL == 'file':
        f = open(fname, "w+")
        f2 = open(fname2, "w+")
    elif PROTOCOL == 's3a':
        f = s3.open(fname, "ab")
        f2 = s3.open(fname2, "ab")
    params = { "mtime_min":"123456789", "mtime_max":now, "loadmetadata":"browse"}
    for k in n.listKeysIter(extra_params=params):
        if len(k.split(",")[0]) > 30 :
            #klist.append([k.rstrip().split(',')[i] for i in [0,1,2,3] ])
            data = [ k.rstrip().split(',')[i] for i in [0,1,2,3] ]
            ringkey = data[0]
            if re.search(arcdatakeypattern, ringkey):
                status, objectkey = revlookupid(ringkey, n)
                data.append(str(objectkey))
            else:
                data.append('0')
            # data.append(str(row.ip))
            # data.append(str(row.chordport))
            # if re.search(arcdatakeypattern, data[0]):
            #     stat = n.chunkapiStoreOp(op='stat', key=data[0], dso=RING, extra_params={'use_base64': '1'})
            #     for s in stat.findall("result"):
            #         status = s.find("status").text
            #         if status == "CHUNKAPI_STATUS_OK":
            #             usermd = s.find("usermd").text
            #             if usermd is not None:
            #                 use_base64 = False
            #                 try:
            #                     use_base64 = s.find("use_base64").text
            #                     if int(use_base64) == 1:
            #                         use_base64 = True
            #                 except:
            #                     pass
            #                 if use_base64 is True:
            #                     rawusermd = base64.b64decode(usermd)
            #                     objectkeyinbytes = struct.unpack(">BBBBBBBBBBBBBBBBBBBB", rawusermd[21:41])
            #                     objectkeylist = []
            #                     for x in objectkeyinbytes:
            #                         raw = '{:02X}'.format(x)
            #                         objectkeylist.append(raw)
            #                     objectkey = ''.join(objectkeylist)
            #                     data.append(str(objectkey))
            # else:
            #     data.append('Null')
            data = ",".join(data)
            print >> f , data

    return [( row.ip, row.adminport, 'OK')]

now = int(str(time.time()).split('.')[0]) - RETENTION
prepare_path()
s = Supervisor(url=URL, login=USER, passwd=PASSWORD)
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])
df = spark.createDataFrame(listm)
print df.show(36, False)
dfnew = df.repartition(36)
listfullkeys = dfnew.rdd.map(lambda x:listkeys(x, now))
dfnew = listfullkeys.flatMap(lambda x: x).toDF()
dfnew.show(1000)
dfnew.write.format("csv").mode("overwrite").options(header="true").save(f2)
