import os
import time
import requests
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

spark = SparkSession.builder.appName("Remove Orphans").getOrCreate()

"""
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)
"""

def deletekey(row):
	key = row._c0
        r = requests.delete('http://127.0.0.1:81/proxy/arc/'+str(key.zfill(40)))
	return [{ "key":key, "code":r.status_code}]

def check_key(key):
        r = requests.head('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)))
        return r.status_code

def getarcid(row):
	key = row._c1
        header = {}
        header['x-scal-split-policy'] = "raw"
	r = requests.head('http://127.0.0.1:81/rebuild/arcdata/'+str(key.zfill(40)))
	print key , r.status_code
	if r.status_code == 200:
		return (key,"OK")
	elif r.status_code == 422:
		return (key,"CORRUPTED")
	else:
		return(key,"UNKNOWN|RING_FAILURE|SREBUILDD_DOWN")

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listkeys.csv/")

#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/list-orphan.csv")

dfARC = df.filter(df["_c1"].rlike(r".*70$"))
dfcARC = dfARC.groupBy("_c1").count().filter("count < 4")

corrupted = dfcARC.rdd.map(getarcid)
print corrupted.collect()
corruptednew = corrupted.toDF()
print corruptednew.show(10,False)

df_final_all = corruptednew.filter(corruptednew["_2"] == "CORRUPTED")
print df_final_all.show(10,False)

filenamearc = "s3a://sparkoutput/output-spark-ARCORPHAN.csv"
df_final_all.write.format('csv').mode("overwrite").options(header='false').save(filenamearc)

#rdd = df_final_all.rdd.map(deletekey)
#rddnew = rdd.flatMap(lambda x: x).toDF()
#deletedorphans = "s3a://sparkoutput/output-spark-DELETED-ARCORPHAN.csv"
#rddnew.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)
