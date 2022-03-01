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

config_path = "%s/%s" % ( sys.path[0] ,"config/config.yml")
with open(config_path, 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

script=options.script
opt=options.ring
opt2=options.extra
localdir = "%s/tmp/" % "/var"
total_cores = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

cmd = "./spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master %s \
        --driver-memory=10g \
        --executor-memory=10g \
	--total-executor-cores=%s \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	--conf spark.hadoop.fs.s3a.access.key=%s \
	--conf spark.hadoop.fs.s3a.secret.key=%s \
        --conf spark.worker.cleanup.enabled=true \
        --conf spark.worker.cleanup.interval=60 \
        --conf spark.worker.cleanup.appDataTtl=604800 \
	--conf spark.hadoop.fs.s3a.endpoint=%s \
	--conf spark.local.dir=%s \
        --jars file:/root/spark/aws-java-sdk-1.7.4.jar,file:/root/spark/hadoop-aws-2.7.3.jar \
        --driver-class-path=/root/spark/aws-java-sdk-1.7.4.jar:/root/spark/hadoop-aws-2.7.3.jar \
	./%s %s %s" % ( cfg["master"], total_cores, cfg["s3"]["access_key"] , cfg["s3"]["secret_key"] , cfg["s3"]["endpoint"] , localdir, script , opt, opt2 )

os.system(cmd)
