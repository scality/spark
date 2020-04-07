from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests
import re
import base64
import hashlib
import binascii
import time

spark = SparkSession.builder.appName("Check Split Objects").getOrCreate()
"""

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)
"""


RING = "IT"

if len(sys.argv)> 1:
	RING = sys.argv[1]

def pad2(n):
  x = '%s' % (n,)
  return ('0' * (len(x) % 2)) + x

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
	key = key.lstrip('0')
        key = pad2(key)
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

def decode_video(r):
        usermd = r.headers['X-Scal-Usermd']
        b64 = base64.b64decode(usermd)
        s = re.findall(r'(V0004sizeL)([0-9]+)',b64)
	video = str("video"+s[0][1])
        return video

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
        if arc:
                r = requests.head('http://127.0.0.1:81/rebuild/arcdata/'+str(key.zfill(40)))
                if r.status_code == 200:
                        keytext = r.headers["X-Scal-Attr-Object-Id"]
                        s = re.findall(r'(text=)([0-9-A-F]+)',keytext)
                        key = s[0][1]
                        video =  decode_video(r)
                        return (key,video)
		else:
			return (key,'KO')
        else:
                r = requests.head('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)),headers=header)
                if r.status_code == 200:
                        video =  decode_video(r)
                        return (key,video)
		else:
			return (key,'KO')

def blob(row):
	key = row._c1
	header = {}
        header['x-scal-split-policy'] = "raw"
	arc,key,video = checkarc(key)
        r = requests.get('http://127.0.0.1:81/proxy/'+str(arc)+'/'+str(key.zfill(40)),headers=header,stream=True)
	if r.status_code == 200:
		chunks = ""
		for chunk in r.iter_content(chunk_size=1024000000):
			if chunk:
				chunks=chunk+chunk

		chunkshex =  chunks.encode('hex')
		rtlst = []
		for k in list(set(sparse(chunkshex))):
			rtlst.append({"key":key,"subkey":k,"digkey":gen_md5_from_id(k)[:26],"size":video})
		return rtlst
	else:
		return [{"key":key,"subkey":"KO","digkey":"KO","size":"KO"}]


files = "file:///fs/spark/listkeys-%s.csv" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

df_split = df.filter(df["_c1"].rlike(r".*000000..5.........$") & df["_c3"].rlike("32")).select("_c1")

df_split.show(20,False)

dfARCsingle = df_split.filter(df["_c1"].rlike(r".*70$"))
dfARCsingle = dfARCsingle.groupBy("_c1").count().filter("count > 3")

dfARCsingle.show(20,False)

dfCOSsingle = df_split.filter(df["_c1"].rlike(r".*20$"))
dfCOSsingle = dfCOSsingle.groupBy("_c1").count()

dfCOSsingle.show(20,False)

dfARCsingle = dfARCsingle.union(dfCOSsingle)

dfARCsingle.show(20,False)
mainchunk = "file:///fs/spark/output/output-single-MAIN-%s.csv" % RING
dfARCsingle.write.format('csv').mode("overwrite").options(header='false').save(mainchunk)

rdd = dfARCsingle.rdd.map(lambda x : blob(x))
rrdnew = rdd.flatMap(lambda x: x)
dfnew = rdd.flatMap(lambda x: x).toDF()
print dfnew.show(20,False)
single = "file:///fs/spark/output/output-single-%s.csv" % RING
dfnew.write.format('csv').mode("overwrite").options(header='false').save(single)

df_sync = df.filter(df["_c1"].rlike(r".*000000..5.........$") & df["_c3"].rlike("16")).select("_c1")

dfARCSYNC = df_sync.filter(df["_c1"].rlike(r".*70$"))
dfARCSYNC = dfARCSYNC.groupBy("_c1").count().filter("count > 3")
dfARCSYNC = dfARCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))


dfCOCSYNC = df_sync.filter(df["_c1"].rlike(r".*20$"))
dfCOCSYNC = dfCOCSYNC.groupBy("_c1").count()
dfCOCSYNC = dfCOCSYNC.withColumn("_c1",F.expr("substring(_c1, 1, length(_c1)-14)"))

dfARCSYNC = dfARCSYNC.union(dfCOCSYNC)

print "SYNC", dfARCSYNC.show(20,False)

singlesync = "file:///fs/spark/output/output-single-SYNC-%s.csv" % RING
dfARCSYNC.write.format('csv').mode("overwrite").options(header='false').save(singlesync)


df2 = dfARCSYNC.withColumnRenamed("_c1","digkey")

inner_join_true =  dfnew.join(df2,["digkey"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('key','size','is_present')
inner_join_false =  dfnew.join(df2,["digkey"], "leftanti").withColumn('is_present', F.lit(int(0))).select('key','size','is_present')

print inner_join_true.show(20,False)
print inner_join_false.show(20,False)

df_final = inner_join_true.union(inner_join_false)
print df_final.show(10,False)

df_all = df_final.groupBy("key").agg(F.sum('is_present').alias('sum'),F.count('is_present').alias('count'),F.max('size').alias('size'))

print df_all.show(10,False)

columns_to_drop = ['count','sum']
df_final_all = df_all.withColumn('good_state', F.when( ( F.col("sum") == F.col("count") ),True).otherwise(False)).drop(*columns_to_drop)

print df_final_all.show(10,False)

all = "file:///fs/spark/output/output-FILE-SHAPE-%s.csv" % RING
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(all)

