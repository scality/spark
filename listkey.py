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

spark = SparkSession.builder.appName("Generate Listkeys").getOrCreate()

RING = "IT"

if len(sys.argv)> 1:
	RING = sys.argv[1]

def listkeys(row, RING):
	klist = []
	n = DaemonFactory().get_daemon("node",login="root", passwd="admin", url='https://{0}:{1}'.format(row.ip, row.adminport), chord_addr=row.ip, chord_port=row.chordport, dso=RING)
	for k in n.listKeysIter():
		if len(k.split(",")[0]) > 30 :
			klist.append([k.rstrip().split(',')[i] for i in [0,1,2,3] ])	
	return klist


s = Supervisor(url="https://sup.scality.com:2443",login="root",passwd="admin")
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])
df = spark.createDataFrame(listm)
print df.show(36,False)
dfnew = df.repartition(8)
listfullkeys = dfnew.rdd.map(lambda x:listkeys(x,RING))
dfnew = listfullkeys.flatMap(lambda x: x).toDF()
listkeys = "file:///fs/spark/listkeys-%s.csv" % RING 
dfnew.write.format('csv').mode("overwrite").options(header='false').save(listkeys)
