"""Submit a spark job to the cluster."""
import os
import sys
# pylint: disable=deprecated-module
from optparse import OptionParser
import yaml


PARSER = OptionParser()
PARSER.add_option("-s", "--script", dest="script", default="", help="Script to submit")
PARSER.add_option("-r", "--ring", dest="ring", default="IT", help="RING name")
PARSER.add_option("-x", "--extra", dest="extra", default="", help="Extra parameter")
(OPTIONS, _) = PARSER.parse_args()

CONFIG_PATH = f"{sys.path[0]}/config/config.yml"
with open(CONFIG_PATH, "r", encoding="utf-8") as ymlfile:
    CFG = yaml.load(ymlfile, Loader=yaml.SafeLoader)

SCRIPT = OPTIONS.script
RING = OPTIONS.ring
EXTRA_OPTS = OPTIONS.extra
LOCALDIR = "/var/tmp/"
TOTAL_CORES = int(CFG["spark.executor.instances"]) * int(CFG["spark.executor.cores"])

CMD = (
    "./spark-2.4.3-bin-hadoop2.7/bin/spark-submit --master %s \
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
	./%s %s %s"
    % (
        CFG["master"],
        TOTAL_CORES,
        CFG["s3"]["access_key"],
        CFG["s3"]["secret_key"],
        CFG["s3"]["endpoint"],
        LOCALDIR,
        SCRIPT,
        RING,
        EXTRA_OPTS,
    )
)

os.system(CMD)
