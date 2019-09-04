from pyspark.sql import SparkSession, Row, SQLContext
from pyspark import SparkContext
import os
import requests
import re
import base64
import hashlib
import binascii
import time

spark = SparkSession.builder.getOrCreate()
"""
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)
"""

def to_bytes(h):
        return binascii.unhexlify(h)

def get_digest(name):
      m = hashlib.md5()
      m.update(name)
      digest = bytearray(m.digest())
      return digest

def get_dig_key(name):
      digest = get_digest(name)
      hash_str =  digest[0] << 16 |  digest[1] << 8  | digest[2]
      oid = digest[3] << 56 |  digest[4] << 48 |  \
            digest[5] << 40 | digest[6] << 32 |   \
            digest[7] << 24 |  digest[8] << 16  | digest[9] << 8 | digest[10]
      hash_str = "{0:x}".format(hash_str)
      oid = "{0:x}".format(oid)
      print hash_str,oid
      oid = oid.zfill(16)
      volid = "00000000"
      svcid = "51"
      specific = "102060"
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      print key
      return key.zfill(40)

def gen_md5_from_id(key):
        int_b = to_bytes(key)
        return get_dig_key(int_b)

def sparse(f):
	lst  = []	
	m = re.findall(r'(425a2d44420100000014|80000000002000000000000..)([0-9-a-f]{40})',f)
	for i in m:
		lst.append(i[1].upper())
	return lst

def getpath(key):
	header = {}
        header['x-scal-split-policy'] = "raw"
	r = requests.head('http://127.0.0.1:81/proxy/chord/'+str(key),headers=header,stream=True)	
	if r.status_code == 200:
		usermd = r.headers['X-Scal-Usermd']
		b64 = base64.b64decode(usermd)
		m = re.findall(r'(V0004sizeL)([0-9]+)',b64)
		return m[0][1]
	else:
		return "VIDEOKO"

def blob(row):
	key = row._c1
	header = {}
        header['x-scal-split-policy'] = "raw"
        r = requests.get('http://127.0.0.1:81/proxy/chord/'+str(key),headers=header,stream=True)
	if r.status_code == 200:
		video = getpath(key)
		chunks = ""
		for chunk in r.iter_content(chunk_size=1024000000):
			if chunk:
				chunks=chunk+chunk
			
		chunkshex =  chunks.encode('hex')
		rtlst = []
		for k in list(set(sparse(chunkshex))):
			rtlst.append({"key":key,"subkey":k,"digkey":gen_md5_from_id(k),"size":video})	
		return rtlst
	else:	
		return [{"key":key,"subkey":"KO","digkey":"KO","size":"KO"}]

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/t.csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/all-fixed-light.csv")

dfARCsingle = df.filter(df["_c1"].rlike(r".*000000005.........$") & df["_c3"].rlike("32")).select("_c1").distinct()
#blob(Row(_c1="4085E9E25998A377965600000000005004DE1420"))
rdd = dfARCsingle.rdd.map(lambda x : blob(x))
#print rdd.collect()
rrdnew = rdd.flatMap(lambda x: x)
#print rrdnew.collect()
dfnew = rdd.flatMap(lambda x: x).toDF()
#print dfnew.show(100)
single = "s3a://sparkoutput/output-single-%s.csv" % (time.time())
dfnew.write.format('csv').options(header='false').save(single)

dfARCSYNC = df.filter(df["_c1"].rlike(r".*000000005.........$") & df["_c3"].rlike("16")).select("_c1").distinct()
#print dfARCSYNC.show(10)

singlesync = "s3a://sparkoutput/output-single-SYNC-%s.csv" % (time.time())
dfARCSYNC.write.format('csv').options(header='false').save(singlesync)

diffDF = dfARCSYNC.subtract(dfnew.select("digkey"))

#INNER JOIN df3 = df1.join(df2, [df1.B == df2.B , df1.C == df2.C], how = 'inner' )
#print diffDF.show(10)
diff = "s3a://sparkoutput/output-single-DIFF-%s.csv" % (time.time())
diffDF.write.format('csv').options(header='false').save(diff)

