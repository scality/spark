from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import requests

spark = SparkSession.builder.appName("Check Split Objects P2").getOrCreate()


RING = "IT"

if len(sys.argv)> 1:
	RING = sys.argv[1]

singlesync = "file:///fs/spark/output/output-single-SYNC-%s.csv" % RING
single = "file:///fs/spark/output/output-single-%s.csv" % RING
dfsinglesync = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(singlesync)
dfsingle =  spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(single)

dfsinglesync = dfsinglesync.withColumnRenamed("_c1","digkey")

dfsinglesync.show(10,False)
dfsingle.show(10,False)

inner_join_true =  dfsingle.join(dfsinglesync,["digkey"], "leftsemi").withColumn('is_present', F.lit(int(1))).select('key','size','is_present')
inner_join_false =  dfsingle.join(dfsinglesync,["digkey"], "leftanti").withColumn('is_present', F.lit(int(0))).select('key','size','is_present')

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
df_final_all.write.format('csv').mode("overwrite").options(header='true').save(all)

