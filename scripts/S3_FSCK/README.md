# S3 object consistency check 

## Requirements

* Protection level must be defined in your configuration file for the ARC schema.

```
arc_protection: 8+4
```

* Pull the report-sproxyd-keys docker container.

```
# docker pull patrickdos/report-sproxyd-keys:basic
```

### Run a full listkeys of the customer DATA RING

This step creates the ``<RING_NAME>/listkeys.csv/`` prefix, which contains the CSV files listing all RING keys.
It can be done in parallel of exporting the S3 keys from all the buckets (see next section).

#### First method: with spark

```
# cd spark/scripts
# python submit.my -s listkey.py -r DATA
```
This method must complete with the empty object ``<RING_NAME>/listkeys.csv/__SUCCESS``. If it does not,
switch to the second method.

#### Second method: with ringsh, awk and aws-cli

For each node of the RING:

```
# ringsh -r ${RING} -u ${NODE} node listKeys | awk  -F, 'BEGIN{OFS=","} {print $1,$2,$3,$4}' > listkeys-${NODE}.csv
# aws ... s3 cp listkeys-${NODE}.csv s3://$(spark_dir_path)/${RING}/listkeys.csv/
```

This method doesn't need the ``<RING_NAME>/listkeys.csv/__SUCCESS`` object.

### Export the S3 keys from all the buckets

### Run the image locally on a S3 connector or specify the bucketD url

#### When using file protocol

```
# docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 > $(spark_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
```

* Do not forget the ``--debug`` option. Otherwise it won't print anything.

* ``<start_date>`` and ``end_date`` allow you to give the the oldest and most recent creation date of the S3 objects. Make sure to include all S3 objects available.

#### When using s3 protocol

*Simplest way*: dump all S3 keys locally and upload them to the S3 bucket.

```
# docker run --net=host patrickdos/report-sproxyd-keys:basic --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 > /var/tmp/s3_keys.txt
# aws s3 cp /var/tmp/s3_keys.txt s3://$(spark_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
```

* Do not forget the ``--debug`` option. Otherwise it won't print anything.

* ``<start_date>`` and ``end_date`` allow you to give the the oldest and most recent creation date of the S3 objects. Make sure to include all S3 objects available.

*Use less disk space with this method:* Configure your aws cli client to work from the same host you run the docker container on. This allows the ability
to stream the data from the container durectly into the bucket. If required change the below example to define any
values that are not stored in your aws configuration. For example setting the profile if it's not the default, or 
setting the endpoint url to use.


```
# docker run --net=host patrickdos/report-sproxyd-keys:basic  --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 | aws s3 cp - s3://$(spark_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
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

### Translate the ARC S3 keys to RING keys

```
# python submit.my -s S3_FSCK/s3_fsck_p0.py -r DATA
```

#### Make sure a driver ARC is properly configured on the spark config points to it
```
http://127.0.0.1:81/rebuild/arc
```

### Filter the listkeys to return only the SPROXY main_chunk + sproxyd single keys

This step is the longest. For each sproxyd key listed in ``<RING_NAME>/s3-bucketd/``, spark workers
send a ``HEAD`` request to their local srebuildd. Each spark worker scans 2,7M keys a day with spark.executor.cores=3
and spark.executor.instances=6

```
# python submit.my -s S3_FSCK/s3_fsck_p1.py -r DATA
```

This step creates the ``<RING_NAME>/s3fsck/arc-keyss3objects-missing.csv/`` prefix.

### Return all the RING keys that are not indexed by S3

```
# python submit.my -s S3_FSCK/s3_fsck_p2.py -r DATA
```

This step creates the ``<RING_NAME>/s3fsck/s3objects-missing.csv/`` prefix.

### Return all the capacity taken by the orphans RING keys that are not indexed by S3
```
#/root/spark_env/bin/python /root/spark/scripts/S3_FSCK/s3_fsck_p3.py DATA
```

This step doesn't create any S3 object but prints an estimate total size of the orphans
that are to be deleted.

### Remove all the RING keys that are not indexed by S3

```
# python submit.my -s S3_FSCK/s3_fsck_p4.py -r DATA
```

## Output / Storage

All files are written to a unified path in SOFS or S3 Buckets. The config.yml file defines the 
PATH and the RING variables. All output from the below scripts will be found inside /${PATH}/${RING}.

![Script Workflow Expected Output](https://raw.githubusercontent.com/scality/spark/master/scripts/S3_FSCK/workflow_diagram.png?token=AEIJKPZVO3EOGBCLLIGENPDBMHLWA)
