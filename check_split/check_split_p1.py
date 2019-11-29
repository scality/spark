from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import re
import requests
import binascii
import hashlib
import base64

spark = SparkSession.builder.appName("Check Split Objects P1").getOrCreate()


RING = "IT"

if len(sys.argv)> 1:
	RING = sys.argv[1]

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

def decode_video(r):
        usermd = r.headers['X-Scal-Usermd']
        b64 = base64.b64decode(usermd)
        s = re.findall(r'(V0004sizeL)([0-9]+)',b64)
        video = str("video"+s[0][1])
        return video

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
			rtlst.append({"key":key,"subkey":k,"digkey":gen_md5_from_id(k)[:-14],"size":video})
		return rtlst
	else:
		return [{"key":key,"subkey":"KO","digkey":"KO","size":"KO"}]

files = "file:///fs/spark/output/output-single-MAIN-%s.csv" % RING
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(files)

df.show(10,False)
rdd = df.rdd.map(lambda x : blob(x))
dfnew = rdd.flatMap(lambda x: x).toDF()
print dfnew.show(20,False)

single = "file:///fs/spark/output/output-single-%s.csv" % RING
dfnew.write.format('csv').mode("overwrite").options(header='true').save(single)

