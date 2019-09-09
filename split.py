from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import requests
import re
import base64
import hashlib
import binascii
import time

#spark = SparkSession.builder.appName("Check Split Objects").getOrCreate()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)


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
      oid = oid.zfill(16)
      volid = "00000000"
      svcid = "51"
      specific = "102060"
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      return key.zfill(40)

def gen_md5_from_id(key):
        int_b = to_bytes(key)
        return get_dig_key(int_b)


def sparse(f):
	lst  = []
	m = re.findall(r'(200000000000014|20100000014)([0-9-a-f]{40})',f)
	n = re.findall(r'(200000000000013|20100000013)([0-9-a-f]{38})',f)
	o = re.findall(r'(200000000000012|20100000012)([0-9-a-f]{36})',f)
	marc =  re.findall(r'(51d68800000014)([0-9-a-f]{40})',f)
	narc =  re.findall(r'(51d68800000013)([0-9-a-f]{38})',f)
	oarc =  re.findall(r'(51d68800000012)([0-9-a-f]{36})',f)
	for mm in m:
		key = mm[1]
		lst.append(key.upper())
	for nn in n:
		key = nn[1]
		lst.append(key.upper())
	for oo in o:
		key = oo[1]
		lst.append(key.upper())
	for mmarc in marc:
		key = mmarc[1]
		lst.append(key.upper())
	for nnarc in narc:
		key = nnarc[1]
		lst.append(key.upper())
	for ooarc in oarc:
		key = oarc[1]
		lst.append(key.upper())
	return lst

def checkarc(key):

	if key[-2:] == "70":
		key, video = getarcid(key,True)
		return ("arc",key , video)
	else:
		try:
			key , video = getarcid(key)
			return ("chord",key , video)
		except Exception as e:
			return ("chord",key,"KO")

def getarcid(key,arc=False):
        header = {}
        header['x-scal-split-policy'] = "raw"
        r = requests.head('http://127.0.0.1:81/proxy/chord/'+str(key),headers=header,stream=True)
        if r.status_code == 200:
                usermd = r.headers['X-Scal-Usermd']
                b64 = base64.b64decode(usermd)
		if arc:
			h64 = b64.encode('hex')
			m = re.findall(r'(00000001000080007b9c6d03000000010000000014)([0-9-a-f]{40})',h64)
			o = re.findall(r'(00000001000080007b9c6d03000000010000000013)([0-9-a-f]{38})',h64)
			n = re.findall(r'(00000001000080007b9c6d03000000010000000012)([0-9-a-f]{36})',h64)
			if m:
				key = m[0][1]
			if o:
				key = o[0][1]
			if n:
				key = n[0][1]
                s = re.findall(r'(V0004sizeL)([0-9]+)',b64)
		video = s[0][1]
		return (key,video)
	else:
		return (key,"KO")

def blob(row):
	key = row._c1
	header = {}
        header['x-scal-split-policy'] = "raw"
	arc,key,video = checkarc(key)
        r = requests.get('http://127.0.0.1:81/proxy/'+str(arc)+'/'+str(key),headers=header,stream=True)
	if r.status_code == 200:
		chunks = ""
		for chunk in r.iter_content(chunk_size=1024000000):
			if chunk:
				chunks=chunk+chunk

		chunkshex =  chunks.encode('hex')
		rtlst = []
		for k in list(set(sparse(chunkshex))):
			rtlst.append({"key":key,"subkey":k,"digkey":gen_md5_from_id(k)[:-14],"size":video})
		return rtlst
	else:
		return [{"key":key,"subkey":"KO","digkey":"KO","size":"KO"}]


df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/list.csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listkeys-IT-5.csv/")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/t.csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/all-fixed-light.csv")

dfARCsingle = df.filter(df["_c1"].rlike(r".*000000005.........$") & df["_c3"].rlike("32")).select("_c1").distinct()

mainchunk = "s3a://sparkoutput/output-single-MAIN.csv"
dfARCsingle.write.format('csv').mode("overwrite").options(header='false').save(mainchunk)

rdd = dfARCsingle.rdd.map(lambda x : blob(x))
rrdnew = rdd.flatMap(lambda x: x)
dfnew = rdd.flatMap(lambda x: x).toDF()
print dfnew.show(20,False)
single = "s3a://sparkoutput/output-single.csv"
dfnew.write.format('csv').mode("overwrite").options(header='false').save(single)

dfARCSYNC = df.filter(df["_c1"].rlike(r".*000000005.........$") & df["_c3"].rlike("16")).select("_c1").distinct()
dfARCSYNC = dfARCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

print "SYNC", dfARCSYNC.show(20,False)

singlesync = "s3a://sparkoutput/output-single-SYNC.csv"
dfARCSYNC.write.format('csv').mode("overwrite").options(header='false').save(singlesync)


df2 = dfARCSYNC.withColumnRenamed("_c1","digkey")

inner_join_true =  dfnew.join(df2,["digkey"], "left_semi").withColumn('is_present', F.lit(True)).select('key','size','is_present').dropDuplicates()
inner_join_false =  dfnew.join(df2,["digkey"], "leftanti").withColumn('is_present_false', F.lit(False)).select('key','is_present_false').dropDuplicates()

print inner_join_true.show(20,False)
print inner_join_false.show(20,False)

inner_join_false = inner_join_false.withColumnRenamed("key","false_key")
df_final = inner_join_true.join(inner_join_false, inner_join_true.key == inner_join_false.false_key,"left")
print df_final.show(10,False)
columns_to_drop = ['is_present_false', 'is_present','false_key']
df_all = df_final.withColumn('good_state', F.when( ( df_final.is_present_false == 'false' ),False).otherwise(True)).drop(*columns_to_drop)
print df_all.show(10,False)

all = "s3a://sparkoutput/output-FILE-SHAPE.csv"
df_all.write.format('csv').mode("overwrite").options(header='false').save(all)

