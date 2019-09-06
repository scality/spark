from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import requests
import re
import base64
import time
import sys

if len(sys.argv)> 1:
	local = False
else:
	local = True

if local is True:
	print "RUN locally"
	os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
	sc = SparkContext('local','example')

	sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
	sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
	sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
	sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
	spark = SQLContext(sc)
else:
	print "RUN on cluster"
	spark = SparkSession.builder.getOrCreate()

#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")
#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/all-fixed-light.csv")
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listkeys-IT.csv/*")

#print df.groupBy("_c3").count().show(32)
print df.groupBy("_c3").agg(F.countDistinct("_c1")).show() 
