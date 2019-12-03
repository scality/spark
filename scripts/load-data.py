import os
import time
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .getOrCreate()


#df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://spark/listmIT-node0[1-6]-n[1-6].csv")
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("s3a://video/all-fixed-light.csv")


dfARCsingle = df.filter(df["_c1"].rlike(r".*5100000000.*") & df["_c3"].rlike("32")).select("_c1").distinct()

#dfu2 = df.select("_c1").distinct()

print dfARCsingle.show()

filenamearcsingle = "s3a://sparkoutput/output-spark-ARCSINGLE-%s.csv" % (time.time())
dfARCsingle.write.format('csv').options(header='false').save(filenamearcsingle)
