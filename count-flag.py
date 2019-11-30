from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import sys

spark = SparkSession.builder.appName("Count flags").getOrCreate()


RING = "IT"
if len(sys.argv)> 1:
        RING = sys.argv[1]

files = "file:///fs/spark/listkeys-%s.csv" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

print df.groupBy("_c3").agg(F.countDistinct("_c1")).show() 
