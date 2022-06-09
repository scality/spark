#!/usr/bin/python2.7
'''
Read keys from stdin and tries to find them by running listKeys on their node.
'''

import sys, os, getopt , re

sys.path.insert(0,'scality')
import subprocess

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.common import ScalDaemonExceptionCommandError
from scality.key import Key


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

files = "%s://%s/%s/s3fsck/recover.csv" % (PROTOCOL, PATH, RING)

def usage(output):
    output.write("""Usage: %s [options]
        Options:
        -h|--help                    Show this help message
        -r|--ring ring name
        -s|--supurl Supervisor Url
        -k|--key objID key of the RS2 object to undelete

""" % os.path.basename(sys.argv[0]))

if __name__ == "__main__":
    options="hr:s:k:"
    long_options=["help", "ring=","supurl=","key="]

    try:
        opts, args = getopt.getopt(sys.argv[1:], options, long_options)
    except getopt.GetoptError, err:
        sys.stderr.write("getopt error %s" % err)
        usage(sys.stderr)
        sys.exit(2)

    ring = None
    sup = None
    key = None
    login = "root"
    password = "admin"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        elif o in ("-r", "--ring"):
            ring = a
        elif o in ("-s", "--supurl"):
            sup = a
        elif o in ("-k", "--key"):
            key = Key(a)
        else:
            usage(sys.stderr)
            sys.exit(2)

    if not ring:
        usage(sys.stderr)
        sys.exit(2)

    if not sup:
    sup = 'http://10.9.31.198:5580'


    s = Supervisor(url=sup,login=login,passwd=password)
    nodes = {}
    success = True
    node =  None
    arck = None

    for n in s.supervisorConfigDso(dsoname=ring)['nodes']:
    nid = '%s:%s' % (n['ip'], n['chordport'])
    nodes[nid] = DaemonFactory().get_daemon("node", login=login, passwd=password, url='https://{0}:{1}'.format(n['ip'], n['adminport']), chord_addr=n['ip'], chord_port=n['chordport'], dso=ring)
    if not node: node = nodes[nid]

    def undeletekey(row):
        key = row.ringkey
        print "Key to Analyse:", key.getHexPadded()
        v = subprocess.Popen('scalarcdig -b '+nid+' '+key.getHexPadded() , shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in v.stdout.readlines():
        if "objkey"  in line:
            try:
                sarc = Key(line[8:])
            except Exception as e:
                print "RINGFAILURE" , e
                break
            key_list = [ sarc ] + [ x for x in sarc.getReplicas() ]
                for arck in key_list :
                check = nodes[node.findSuccessor(arck.getHexPadded())["address"]]
                tab  = check.checkLocal(arck.getHexPadded())
                print  "%s;%s;%s" % ( key.getHexPadded() , arck.getHexPadded() , tab )
                if tab["deleted"] == True:
                    print "Undelete Key " , arck.getHexPadded()
                    version = int(tab["version"]+64)
                    try:
                        check.chunkapiStoreOp(op="undelete", key=arck.getHexPadded(), extra_params={"version": version})
                    except ScalFactoryExceptionTypeNotFound as e:
                         print "Error %s " , e

    df = spark.read.format("csv").option("header",
                                         "false").option("inferSchema",
                                                         "true").load(files)
    df = df.withColumnRenamed("_c0", "ringkey")
    df = df.repartition(PARTITIONS)
    rdd = df.rdd.map(undeletekey).toDF()
    recoveredorphans = "%s://%s/%s/s3fsck/recovered-ring-keys.csv" % (
    PROTOCOL, PATH, RING)
    rdd.write.format("csv").mode("overwrite").options(header="false").save(recoveredorphans)

    sys.exit(0 if success else 1)
