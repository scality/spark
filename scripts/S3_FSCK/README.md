# S3 object consistency check 

## Requirements

* Protection level must be defined in your configuration file for the ARC schema.

```
arc_protection: 8+4
```


### Run a full listkeys of the customer DATA RING
```
#/root/spark_env/bin/python /root/spark/scripts/listkey.py DATA
```

### Export the S3 keys from all the bucket

#### Pull the docker images
```
#docker pull patrickdos/report-sproxyd-keys:basic
```

### Run the image locally on a S3 connector or specify the bucketD url
```
#docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s http://127.0.0.1:9000 > $(spar_dir_path)/<RING_NAME>>/s3-bucketd/keys.txt
```

If you use the s3a protocol to use bucket storage, copy the output from the keys.txt file into the bucket. For instance
if your path was spark-results, and the ring was DATA, you would copy the file into the bucket like so:

`aws --endpoint=http://<Endpoint_IP> s3 cp /tmp/keys.txt s3://spark-results/DATA/s3-bucketd/keys.txt`


####Bonus Track you could do it per buckets
```
#docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s http://127.0.0.1:9000 -b video > $(spar_dir_path)/<RING_NAME>>/s3-bucketd/keys.txt
```

### Translate the ARC S3 keys to RING keys

```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/check_files_p0.py DATA
```

#### Make sure a driver ARC is properly configured on the spark config points to it
```
http://127.0.0.1:81/rebuild/arc
```

### Filter the listkeys to return only the SPROXY main_chunk + sproxyd single keys

```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/check_files_p1.py DATA
```

### Return all the RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/check_files_p2.py DATA
```

### Return all the capacity taken by the orphans RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/check_files_p3.py DATA
```

### Remove all the RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/check_files_p4.py DATA
```
