script=$1
./spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master spark://178.33.63.238:7077 \
        --driver-memory=12g \
        --executor-memory=12g \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	--conf spark.hadoop.fs.s3a.access.key=VKIKE9MQ8AM3I5Y0LOZG \
	--conf spark.hadoop.fs.s3a.secret.key=d1EF3mUbLYBp2oezdzdh37RdQPtXHfmmst0R/zd6 \
	--conf spark.hadoop.fs.s3a.endpoint=http://sreport.scality.com \
        --jars file:/root/spark/aws-java-sdk-1.7.4.jar,file:/root/spark/hadoop-aws-2.7.3.jar \
        --driver-class-path=/root/spark/aws-java-sdk-1.7.4.jar:/root/spark/hadoop-aws-2.7.3.jar \
	./$script
