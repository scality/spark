import os
import sys
import shutil
import requests
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

RING = sys.argv[1]
spark = SparkSession.builder.appName("Generate Listkeys ring:"+RING).getOrCreate()

with open("./config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

user = cfg["sup"]["login"]
password = cfg["sup"]["password"]
url = cfg["sup"]["url"]
cpath = cfg["path"]
path = "%s/listkeys-%s.csv/" % (cpath, RING)

def prepare_path():
	shutil.rmtree(path)
	if not os.path.exists(path):
		os.makedirs(path)
	
def listkeys(row):
	klist = []
	n = DaemonFactory().get_daemon("node",login=user, passwd=password, url='https://{0}:{1}'.format(row.ip, row.adminport), chord_addr=row.ip, chord_port=row.chordport, dso=RING)

	fname = "%s/node-%s-%s.csv" % (path , row.ip, row.chordport )
	f = open(fname,"w+")
	for k in n.listKeysIter():
		if len(k.split(",")[0]) > 30 :
			#klist.append([k.rstrip().split(',')[i] for i in [0,1,2,3] ])	
			data = [ k.rstrip().split(',')[i] for i in [0,1,2,3] ]
			data = ",".join(data)
			print >> f , data
	return [( row.ip, row.adminport, 'OK')]


s = Supervisor(url=url,login=user,passwd=password)
listm = sorted(s.supervisorConfigDso(dsoname=RING)['nodes'])
df = spark.createDataFrame(listm)
print df.show(36,False)
dfnew = df.repartition(36)
listfullkeys = dfnew.rdd.map(lambda x:listkeys(x))
dfnew = listfullkeys.flatMap(lambda x: x).toDF()
dfnew.show(1000)
