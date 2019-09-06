import os
import time
import requests
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext

spark = SparkSession.builder.appName("Check Orphans").getOrCreate()

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
        r = requests.delete('http://127.0.0.1:81/proxy/chord/'+str(key))
	return [{ "key":key, "code":r.status_code}]

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listkeys-IT.csv/*")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/orphan.cvs")

dfARC = df.filter(df["_c1"].rlike(r".*70$"))
dfcARC = dfARC.groupBy("_c1").count().filter("count < 3")

df1 = df.withColumnRenamed("_c1","df1_c1")
df2 = dfcARC.withColumnRenamed("_c1","df2_c1")
inner_join = df1.join(df2,df2.df2_c1==df1.df1_c1).select('_c0')

rdd = inner_join.rdd.map(deletekey)
rddnew = rdd.flatMap(lambda x: x).toDF()

filenamearc = "s3a://sparkoutput/output-spark-ARCORPHAN-%s.csv" % (time.time())
dfcARC.write.format('csv').options(header='false').save(filenamearc)
inner = "s3a://sparkoutput/output-spark-ARCORPHAN-JOIN-%s.csv" % (time.time())
inner_join.write.format('csv').options(header='false').save(inner)

deletedorphans = "s3a://sparkoutput/output-spark-DELETED-ARCORPHAN-%s.csv" % (time.time())
rddnew.write.format('csv').options(header='false').save(deletedorphans)
