import sys
import yaml
import os
from optparse import OptionParser
import subprocess
from collections import deque

# Parsing command-line options
parser = OptionParser()
parser.add_option("-s", "--script", dest="script", default="", help="Script to submit")
parser.add_option("-r", "--ring", dest="ring", default="IT", help="RING name")
parser.add_option("-x", "--extra", dest="extra", default="", help="Extra parameter")
(options, args) = parser.parse_args()

# Load configuration from config.yml
config_path = os.path.join(sys.path[0], "config/config.yml")
with open(config_path, "r") as ymlfile:
   cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

script_path = options.script
arg1 = options.ring
arg2 = options.extra
localdir = "/var/tmp/spark-local-dir"
total_cores = int(cfg["spark.executor.instances"]) * int(cfg["spark.executor.cores"])

def tail(filename, n=10):
    with open(filename) as f:
        return deque(f, n)

def run_script(worker_id, script_path, flag1, flag2):
   cmd = f"spark-submit --master {cfg['master']} \
      --conf spark.executor.instances={cfg['spark.executor.instances']} \
      --conf spark.executor.cores={cfg['spark.executor.cores']} \
      --conf spark.driver.host={cfg['spark.driver.bindAddress']} \
      --conf spark.driver.bindAddress={cfg['spark.driver.bindAddress']} \
      --conf spark.worker.cleanup.enabled=false \
      --conf spark.worker.cleanup.interval=60 \
      --conf spark.worker.cleanup.appDataTtl=604800 \
      --conf spark.memory.offHeap.enabled={str(cfg['spark.memory.offHeap.enabled']).lower()} \
      --conf spark.memory.offHeap.size={cfg['spark.memory.offHeap.size']} \
      --conf spark.local.dir='{localdir}' \
      --total-executor-cores={total_cores} \
      --driver-memory={cfg['spark.driver.memory']} \
      --executor-memory={cfg['spark.executor.memory']} \
      --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
      --conf spark.hadoop.fs.s3a.access.key={cfg['s3']['access_key']} \
      --conf spark.hadoop.fs.s3a.secret.key={cfg['s3']['secret_key']} \
      --conf spark.hadoop.fs.s3a.endpoint={cfg['s3']['endpoint']} \
      --jars file:/spark/jars/aws-java-sdk-1.12.770.jar,file:/spark/jars/hadoop-aws-3.3.4.jar \
      --driver-class-path=/spark/jars/aws-java-sdk-1.12.770.jar:/spark/jars/hadoop-aws-3.3.4.jar \
      --deploy-mode client \
      {script_path} {flag1} {flag2}"

   # Define file paths for stdout and stderr
   stdout_file = f"/var/tmp/worker_{worker_id}_stdout.log"
   stderr_file = f"/var/tmp/worker_{worker_id}_stderr.log"

   print(f"Running command: {cmd}")
   with open(stdout_file, "w") as stdout_f, open(stderr_file, "w") as stderr_f:
      process = subprocess.Popen(cmd, shell=True, stdout=stdout_f, stderr=stderr_f)
      process.communicate()

   if process.returncode == 0:
      print(f"Worker {worker_id} executed {script_path} successfully. Output extract:\n")
      for line in tail(stdout_file, 10):
         print(line.strip()[:300])
      print("\n")
      return f"Worker {worker_id} executed {script_path} successfully. Output written to {stdout_file}."
   else:
      return f"Worker {worker_id} failed to execute {script_path}. Check {stderr_file} for errors."

res = run_script(0, script_path, arg1, arg2)
print(res)
