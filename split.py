from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext
import os
import requests
import re
import time

#spark = SparkSession.builder.getOrCreate()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)

def sparse(f):
	lst  = []	
	m = re.findall(r'(425a2d44420100000014|8000000000200000000000014)([0-9-a-f]{40})',f)
	for i in m:
		lst.append(i[1].upper())
	return lst

def blob(row):
	key = row._c1
	header = {}
        header['x-scal-split-policy'] = "raw"
        r = requests.get('http://127.0.0.1:81/proxy/chord/'+str(key),headers=header,stream=True)
	if r.status_code == 200:
		chunks = ""
		for chunk in r.iter_content(chunk_size=1024):
			if chunk:
				chunks=chunk+chunk
			
		chunkshex =  chunks.encode('hex')
		rtlst = []
		for k in list(set(sparse(chunkshex))):
			rtlst.append({"key":key,"subkey":k})	
		return rtlst

#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/all-fixed-light.csv")

dfARCsingle = df.filter(df["_c1"].rlike(r".*5100000000.*") & df["_c3"].rlike("32")).select("_c1").distinct()

#blob(Row(_c1="4085E9E25998A377965600000000005004DE1420"))
rdd = dfARCsingle.rdd.map(lambda x : blob(x))

print rdd.collect()
rrdnew = rdd.flatMap(lambda x: x)
#print rrdnew.collect()
dfnew = rdd.flatMap(lambda x: x).toDF()
print dfnew.show(10)

single = "s3a://sparkoutput/output-single-%s.csv" % (time.time())
dfnew.write.format('csv').options(header='false').save(single)

