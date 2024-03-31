from pyspark.sql import SparkSession, Row, SQLContext
import pyspark.sql.functions as F
from pyspark import SparkContext
import os
import sys
import subprocess
import yaml
import random
import logging

config_path = "%s/%s" % (sys.path[0], "../config/config.yml")
with open(config_path, "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

if len(sys.argv) >1:
    RING = sys.argv[1]
else:
    RING = cfg["ring"]

PATH = cfg["path"]
PROTOCOL = cfg["protocol"]
ACCESS_KEY = cfg["s3"]["access_key"]
SECRET_KEY = cfg["s3"]["secret_key"]
ENDPOINT_URL = cfg["s3"]["endpoint"]
REVLOOKUP_BIN = cfg["nasdk_tools"]["scalarcrevlookupid_bin"]
SPLITGETUSERMD_BIN = cfg["nasdk_tools"]["scalsplitgetusermd_bin"]
PORT_RANGE = cfg["nasdk_tools"]["port_range"]
ARCDATA_DRIVER = cfg["nasdk_tools"]["arcdata_path"]


os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
spark = SparkSession.builder \
     .appName("s3_fsck_p2_reverselookup.py: lookup the input keys for the newly found orphaned DATA RING main chunks. If their service ID is SPLIT, look the top input key up. RING: " + RING) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
     .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
     .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)\
     .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL) \
     .config("spark.executor.instances", cfg["spark.executor.instances"]) \
     .config("spark.executor.memory", cfg["spark.executor.memory"]) \
     .config("spark.executor.cores", cfg["spark.executor.cores"]) \
     .config("spark.driver.memory", cfg["spark.driver.memory"]) \
     .config("spark.memory.offHeap.enabled", cfg["spark.memory.offHeap.enabled"]) \
     .config("spark.memory.offHeap.size", cfg["spark.memory.offHeap.size"]) \
     .config("spark.local.dir", cfg["path"]) \
     .getOrCreate()

#spark.sparkContext.setLogLevel("DEBUG")
#print(os.environ["PATH"])
input_path = "%s://%s/%s/s3fsck/s3objects-missing.csv" % (PROTOCOL, PATH, RING)
output_path = "%s://%s/%s/s3fsck/reverselookup.csv" % (PROTOCOL, PATH, RING)

print("REVLOOKUP_BIN:", REVLOOKUP_BIN)
print("SPLITGETUSERMD_BIN:", SPLITGETUSERMD_BIN)
print("PORT_RANGE:", PORT_RANGE)
print("ARCDATA_DRIVER:", ARCDATA_DRIVER)
def get_local_ip():
    try:
        output = subprocess.check_output("hostname -i", shell=True, stderr=open(os.devnull, 'wb')).decode().strip()
        return output
    except subprocess.CalledProcessError as e:
        return "ERR_IP"

def get_random_port():
    return random.randint(PORT_RANGE[0], PORT_RANGE[1])

def rev_lookup(key):
    try:
        ip_port = "{}:{}".format(get_local_ip(), get_random_port())
        cmd = "ssh -qT $(hostname -i) {} -b '{}' {}".format(REVLOOKUP_BIN, ip_port, key)
        print("  Executing rev_lookup command: {}".format(cmd))
        output = subprocess.check_output(cmd, shell=True, stderr=open(os.devnull, 'wb'))
        print("  rev_lookup output: {}".format(output))
        if output:
            return output.decode().strip()
        else:
            return "NULL"
    except subprocess.CalledProcessError as e:
        print("  rev_lookup error: {}".format(e.output.decode()))
        return "ERR_REVLOOKUP"

def split_lookup(key):
    try:
        ip_port = "{}:{}".format(get_local_ip(), get_random_port())
        cmd = "ssh -qT $(hostname -i) {} -b '{}' -d '{}' {} 2>/dev/null | strings | awk '/index/{{$1=substr($1,length($1)-39,40); print}}'".format(SPLITGETUSERMD_BIN, ip_port, ARCDATA_DRIVER, key)
        print("  Executing split_lookup command: {}".format(cmd))
        output = subprocess.check_output(cmd, shell=True, stderr=sys.stdout)
        print("  split_lookup output: {}".format(output))
        if output:
            return output.decode().strip()
        else:
            return "NULL"
    except subprocess.CalledProcessError as e:
        print("  split_lookup error: {}".format(e.output.decode()))
        return "ERR_SPLITUSERMD"

# try a reverse look up on a RING key; if the result is an sproxyd input key matching a SPLIT service ID (hardcoded here as /4[CD]/), try a scalsplitgetusermd to look the top input key up.
def process_key(key):
    print("  0. enter process")
    rev_lookup_result = rev_lookup(key)
    print("  1. rev_lookup result: " + rev_lookup_result)

    if rev_lookup_result and rev_lookup_result[30:32] in ['4C', '4D']:
        split_lookup_result = split_lookup(key)
        print("  2. split_lookup result: " + split_lookup_result)
        return key, rev_lookup_result, split_lookup_result
    else:
        return key, rev_lookup_result, "NULL"

def test_worker_command(key):
    worker_result = get_local_ip()
    return key, worker_result

def test_rev_lookup():
    key = "FFF1D15C4FE78E7138414000000000511470C070"
    result = rev_lookup(key)
    print("test_rev_lookup result:", result)

    # Add your assertions here to verify the result
    # For example:
    # assert result == "expected_value", "Test failed: rev_lookup() returned an incorrect value"

def test_split_lookup():
    key = "FFF1D15C4FE78E7138414000000000511470C070"
    result = split_lookup(key)
    print("test_split_lookup result:", result)

def test_process_key():
    key = "FFF1D15C4FE78E7138414000000000511470C070"
    result = process_key(key)
    print("test_process_key result:", result)

def test_lookup_functions(key):
    process_result = process_key(key)
    return (process_result)

def test_scenario():
    test_keys = ["FFF1D15C4FE78E7138414000000000511470C070", "FFF1D15C4FE78E7138414000000000511470C070", "FFF1D15C4FE78E7138414000000000511470C070"]
    test_df = spark.createDataFrame([(key,) for key in test_keys], ["key"])
    test_ip_df = test_df.rdd.map(lambda row: test_worker_command(row.key)).toDF(["key", "worker result"])
    test_ip_results = test_ip_df.collect()
    print("--> Test worker IP results:")
    for result in test_ip_results:
        print(result)
    
    # Call the unit test functions
    
    # local unit tests
    test_rev_lookup()
    test_split_lookup()
    test_process_key()
    
    # distributed tests
    test_results_df = test_df.rdd.map(lambda row: process_key(row.key)).toDF(["key", "rev_result", "split_result"])
    test_results = test_results_df.collect()
    print("--> Test results:")
    for result in test_results:
        print(result)

df = spark.read.format("csv").option("header", "false").load(input_path)

df_with_lookups = df.rdd.map(lambda row: process_key(row[0])).toDF(["main_chunk", "input_key", "top_input_key"])

df_with_lookups.write.format("csv").mode("overwrite").options(header="false").save(output_path)
