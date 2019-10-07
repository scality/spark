from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext

import os

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.key import Key
from scality.storelib.storeutils import uks_parse

import requests
requests.packages.urllib3.disable_warnings()

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
"""
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)
"""

def listkeys(row):
	klist = []
	n = DaemonFactory().get_daemon("node",login="root", passwd="admin", url='https://{0}:{1}'.format(row.ip, row.adminport), chord_addr=row.ip, chord_port=row.chordport, dso="IT")
	for k in n.listKeysIter():
		if len(k.split(",")[0]) > 30 :
			klist.append([k.rstrip().split(',')[i] for i in [0,1,2,3] ])	
	return klist


s = Supervisor(url="https://sup.scality.com:2443",login="root",passwd="admin")
listm = sorted(s.supervisorConfigDso(dsoname="IT")['nodes'])
df = spark.createDataFrame(listm)
print df.show(36,False)
dfnew = df.repartition(8)
listfullkeys = dfnew.rdd.map(lambda x:listkeys(x))
dfnew = listfullkeys.flatMap(lambda x: x).toDF()
listkeys = "s3a://spark/listkeys.csv" 
dfnew.write.format('csv').mode("overwrite").options(header='false').save(listkeys)
