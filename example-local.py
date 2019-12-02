os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
sc = SparkContext('local','example')

sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'VKIKE9MQ8AM3I5Y0LOZG')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'd1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://sreport.scality.com')
spark = SQLContext(sc)
