from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import sys

spark = SparkSession.builder.appName("Check Split Objects P0").getOrCreate()


RING = "IT"

if len(sys.argv)> 1:
	RING = sys.argv[1]

files = "file:///fs/spark/listkeys-%s.csv" % RING
df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(files)

df_split = df.filter(df["_c1"].rlike(r".*000000..5.........$") & df["_c3"].rlike("32")).select("_c1")

dfARCsingle = df_split.filter(df["_c1"].rlike(r".*70$"))
dfARCsingle = dfARCsingle.groupBy("_c1").count().filter("count > 3")

dfCOSsingle = df_split.filter(df["_c1"].rlike(r".*20$"))
dfCOSsingle = dfCOSsingle.groupBy("_c1").count()

dfARCsingle = dfARCsingle.union(dfCOSsingle)

dfARCsingle.show(20,False)
mainchunk = "file:///fs/spark/output/output-single-MAIN-%s.csv" % RING
dfARCsingle.write.format('csv').mode("overwrite").options(header='true').save(mainchunk)

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
dfARCSYNC.write.format('csv').mode("overwrite").options(header='true').save(singlesync)
