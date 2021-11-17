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

#### When using file protocol
```
#docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 > $(spar_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
```

#### When using s3 protocol
Configure your aws cli client to work from the same host you run the docker container on. This allows the ability
to stream the data from the container durectly into the bucket. If required change the below example to define any
values that are not stored in your aws configuration. For example setting the profile if it's not the default, or 
setting the endpoint url to use.


```
#docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 | aws s3 cp - s3://$(spar_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
```
This method may error if the file being uploaded as a stream exceeds 5GB. This can be mitigated by using the 
--expected-size flag so aws knows the expected size of the file and can calculate the correct size and qty of MPUs for 
the upload. The average line per object is around 100bytes, for a long bucket name this might be 128bytes per object. 
The example below of s3api shows how to count the quantity of objects per bucket, which can be used to help calculate
the --expected-size to provide. 

With a bucket name of 10 characters or less every object will produce a line of output of 100 bytes or less (depends
on the length of the nodes IP Address). So 5GB will be reached/exceeded when a bucket name has length of 10 characters and
50Million objects inside it.

```
aws s3api list-objects --bucket <BUCKET_NAME> --output json --query "[length(Contents[])]"
```


####Bonus Track you could do it per buckets
```
#docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s http://127.0.0.1:9000 -b video > $(spar_dir_path)/<RING_NAME>>/s3-bucketd/keys.txt
```

### Translate the ARC S3 keys to RING keys

```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p0.py DATA
```

#### Make sure a driver ARC is properly configured on the spark config points to it
```
http://127.0.0.1:81/rebuild/arc
```

### Filter the listkeys to return only the SPROXY main_chunk + sproxyd single keys

```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p1.py DATA
```

### Return all the RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p2.py DATA
```

### Return all the capacity taken by the orphans RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p3.py DATA
```

### Remove all the RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p4.py DATA
```

## Output / Storage

All files are written to a unified path in SOFS or S3 Buckets. The config,yml file defines the 
PATH and the RING variables. All output from the below scripts will be found inside /${PATH}/${RING}.

![Script Workflow Expected Output](https://raw.githubusercontent.com/scality/spark/master/scripts/S3_FSCK/workflow_diagram.png?token=AEIJKPZVO3EOGBCLLIGENPDBMHLWA)
