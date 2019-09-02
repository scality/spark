import os
import time
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()


df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")

dfARC = df.filter(df["_c1"].rlike(r".*70$"))
dfCOS = df.filter(df["_c1"].rlike(r".*20$"))
dfcARC = dfARC.groupBy("_c1").count().filter("count < 4")
dfcCOS = dfCOS.groupBy("_c1").count().filter("count < 2")

filenamearc = "s3a://sparkoutput/output-spark-ARC-%s.csv" % (time.time())
filenamecos = "s3a://sparkoutput/output-spark-COS-%s.csv" % (time.time())
dfcARC.write.format('csv').options(header='false').save(filenamearc)
dfcCOS.write.format('csv').options(header='false').save(filenamecos)
