import os
import sys
import requests
requests.packages.urllib3.disable_warnings()

from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.key import Key
from scality.storelib.storeutils import uks_parse


import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

spark = SparkSession.builder.appName("Removes Keys").getOrCreate()

RING = "IT"

if len(sys.argv)> 1:
    RING = sys.argv[1]

def delete_key(key, dsolist, ring):

    node = {}
    nodes = {}
    key = Key(key._c0)

    for n in dsolist:
        nid = '%s:%s' % (n['ip'], n['chordport'])
        try:
            nodes[nid] = DaemonFactory().get_daemon("node", url='https://{0}:{1}'.format(n['ip'], n['adminport']), chord_addr=n['ip'], chord_port=n['chordport'], login="root",passwd="admin", dso=ring)
        except ScalFactoryExceptionTypeNotFound as e :
	    return ({"key":key.getHexPadded() , "status":"FIND_NODE_KO"})
        if not node: node = nodes[nid]
    try:
        check = nodes[node.findSuccessor(key.getHexPadded())["address"]]
    except ScalDaemonExceptionInvalidParameter as e:
	return ({"key":key.getHexPadded() , "status":"CHECK_KO"})
    version = 1000000
    try:
        et = check.chunkapiStoreOp("delete", key=key.getHexPadded(),extra_params={"version": version })
	return ({"key":key.getHexPadded() , "status":et.find("result").find("status").text })
    except Exception as e:
	return ({"key":key.getHexPadded() , "status":"DELETE_KO"})

s = Supervisor(url="https://sup.scality.com:2443",login="root",passwd="admin")
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])

filenamearc = "file:///fs/spark/output/output-spark-ARC-%s.csv" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(filenamearc)

df_keys = df.rdd.map(lambda x:delete_key(x,listm,RING)).toDF()
df_keys.show(10,False)
delete_keys = "file:///fs/spark/output/removed_keys-%s.csv" % RING
df_keys.write.format('csv').mode("overwrite").options(header='false').save(delete_keys)
