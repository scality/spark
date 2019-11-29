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
        r = requests.delete('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)))
        #r = requests.head('http://127.0.0.1:81/proxy/chord/'+str(key.zfill(40)))
	return ( key, r.status_code)

df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("file:///fs/spark/output/output-spark-ARCORPHAN.csv")
rdd = df.rdd.map(deletekey).toDF()
#rddnew = rdd.flatMap(lambda x: x).toDF()
rdd.show(10,False)
deletedorphans = "file:///fs/spark/output/output-spark-DELETED-ARCORPHAN.csv"
rdd.write.format('csv').mode("overwrite").options(header='false').save(deletedorphans)
