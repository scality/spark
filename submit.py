import sys
import yaml
import os

from optparse import OptionParser

parser = OptionParser()
parser.add_option("-s", "--script", dest="script",
   default='', help="Script to submit")
parser.add_option("-r", "--ring", dest="ring",
   default='IT', help="RING name")
parser.add_option("-x", "--extra", dest="extra",
   default='', help="Extra parameter")
(options, args) = parser.parse_args()

with open("./config.yml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

script=options.script
opt=options.ring
opt2=options.extra

cmd = "./spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master %s \
        --driver-memory=10g \
        --executor-memory=10g \
	--total-executor-cores=60 \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	--conf spark.hadoop.fs.s3a.access.key=%s \
	--conf spark.hadoop.fs.s3a.secret.key=%s \
	--conf spark.hadoop.fs.s3a.endpoint=%s \
        --jars file:/root/spark/aws-java-sdk-1.7.4.jar,file:/root/spark/hadoop-aws-2.7.3.jar,file:/root/spark/alluxio-2.1.0-client.jar \
        --driver-class-path=/root/spark/aws-java-sdk-1.7.4.jar:/root/spark/hadoop-aws-2.7.3.jar:/root/spark/alluxio-2.1.0-client.jar \
	./%s %s %s" % ( cfg["master"], cfg["s3"]["access_key"] , cfg["s3"]["secret_key"] , cfg["s3"]["endpoint"] , script , opt, opt2 )

os.system(cmd)
