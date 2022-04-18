import os
import time
import requests
import yaml
import re
import sys
import struct
import base64
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.key import Key
from scality.storelib.storeutils import uks_parse


config_path = "%s/%s" % ( sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.load(ymlfile)


if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

USER = cfg["sup"]["login"]
PASSWORD = cfg["sup"]["password"]
URL = cfg["sup"]["url"]
PATH = cfg["path"]
SREBUILDD_IP  = cfg["srebuildd_ip"]
SREBUILDD_PATH  = cfg["srebuildd_chord_path"]
SREBUILDD_URL = "http://%s:81/%s" % (SREBUILDD_IP, SREBUILDD_PATH)
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
PARTITIONS = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])
ARC = cfg["arc_protection"]

arcindex = {"4+2": "102060", "8+4": "2040C0", "9+3": "2430C0", "7+5": "1C50C0", "5+7": "1470C0"}
arcdatakeypattern = re.compile(r'[0-9a-fA-F]{38}70')

s = Supervisor(url=URL, login=USER, passwd=PASSWORD)
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p4.py:Clean the extra keys :" + RING) \
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


def findsuccessor(key, node):
    """
    Performs an arc reverse lookup id to obtain the arc stripes object key.
    REQUIREMENTS:
    key: A ring_data_key (arc 70)
    node: node from a DaemonFactory().get_daemon("node") assignment
    ring: Name of the ring to perform lookup in.
    """
    successor = node.findSuccessor(key)
    status = successor['status']
    if status:
        ip, chordport = successor['address'].split(':')
        mynode = [x for x in listm if ip in x['ip'] and chordport in x['chordport']]
        adminport = mynode[0]['adminport']
        return (status, ip, adminport, chordport)
    else:
        return status, None, None, None


def revlookupid(ringkey, nodelist, ring):
    """
    Performs an arc reverse lookup id to obtain the arc stripes object key.
    REQUIREMENTS:
    ringkey: A ring_data_key (arc 70)
    nodelist: list of nodes returned from DaemonFactory().get_daemon("node")
    ring: Name of the ring to perform lookup in.
    """
    ip = nodelist[0]['ip']
    adminport = nodelist[0]['adminport']
    chordport = nodelist[0]['chordport']
    lookupnode = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(ip, adminport), chord_addr=ip, chord_port=chordport, dso=ring)
    status, successorip, successoradminport, successorchordport = findsuccessor(ringkey, lookupnode)
    if status:
        node = DaemonFactory().get_daemon("node", login=USER, passwd=PASSWORD, url='https://{0}:{1}'.format(successorip, successoradminport), chord_addr=successorip, chord_port=successorchordport, dso=ring)
        stat = node.chunkapiStoreOp(op='stat', key=ringkey, dso=RING, extra_params={'use_base64': '1'})
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
                        if re.search(arcdatakeypattern, ringkey):
                            objectkeyinbytes = struct.unpack(">BBBBBBBBBBBBBBBBBBBB", rawusermd[21:41])
                        else:
                            objectkeyinbytes = struct.unpack(">BBBBBBBBBBBBBBBBBBBB", rawusermd[03:23])
                        objectkeylist = []
                        for x in objectkeyinbytes:
                            raw = '{:02X}'.format(x)
                            objectkeylist.append(raw)
                        objectkey = ''.join(objectkeylist)
                        while objectkey.endswith('00'):
                            objectkey = objectkey[:-2].zfill(40)
                    return (status, objectkey)
                else:
                    status = "NOK: usermd is None"
                    return (status, None)
            else:
                return (status, None)
    else:
        status = 'NOK: findsuccessor status False'
        return (status, None)

def deletekey(row):
        key = row.ringkey
        try:
                url = "%s/%s" % ( SREBUILDD_URL, str(key.zfill(40)) )
                print(url)
                r = requests.delete(url)
                status_code = r.status_code
                #status_code = "OK"
                return ( key, status_code, url)
        except requests.exceptions.ConnectionError as e:
                return (key,"ERROR_HTTP", url)


files = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)
df = df.withColumnRenamed("_c0","ringkey")
df = df.repartition(PARTITIONS)
rdd = df.rdd.map(deletekey).toDF()
deletedorphans = "%s://%s/%s/s3fsck/deleted-s3-orphans.csv" % (PROTOCOL, PATH, RING)
rdd.write.format("csv").mode("overwrite").options(header="false").save(deletedorphans)
