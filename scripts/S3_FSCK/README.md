# S3 object consistency check 


## Clustering Deployment based on Docker

The spark tool can be used to provide an application level orphan cleanups for S3 connectors. This tool is not for RING orphans.

### Requirements

Pull the docker spark-worker image on the servers you want to act as a spark node.

```
[root@node01 ~]# docker pull tbenson/spark-worker
```

Pull the docker spark-master image on a server ( could be a spark node ).

```
[root@node01 ~]# docker pull tbenson/spark-master
```

### The spark cluster

Warning:
If you choose SOFS, TACO is mandatory, otherwise, will fail when it will create the path to output the results.

The spark cluster must have all nodes defined properly when the container starts. Do not include any nodes which are not participating in the cluster otherwise workers may attempt to use them to shuffle data or you may observe errors in the Spark Application when looking at the logs in the UI.

#### Starting the first worker of a 6 node cluster:

```
docker run --rm -dit  --net=host --name spark-worker \
           --hostname spark-worker  \
           --add-host spark-master:178.33.63.238 \
           --add-host spark-worker:178.33.63.238  \
           --add-host=node01:178.33.63.238  \
           --add-host=node02:178.33.63.219 \
           --add-host=node03:178.33.63.192 \
           --add-host=node04:178.33.63.213 \
           --add-host=node05:178.33.63.77 \
           --add-host=node06:178.33.63.220 \
            tbenson/spark-worker
```

* spark-worker should be the local IP of the node running the container.
* Each of the `--add-host` entries needs to contain the `Shortname:IPAdress` of each spark-worker and should use resolvable shortnames (names exist in /etc/hosts, dns A records, etc.)


#### Starting the master of the 6 node cluster:

```
docker run --rm -dit --net=host --name spark-master \
           --hostname spark-master \
           --add-host spark-master:178.33.63.238 \
           --add-host=node01:178.33.63.238  \
           --add-host=node02:178.33.63.219 \
           --add-host=node03:178.33.63.192 \
           --add-host=node04:178.33.63.213 \
           --add-host=node05:178.33.63.77 \
           --add-host=node06:178.33.63.220 \
           tbenson/spark-master
```

* spark-worker should be the local IP of the node running the container.
* Each of the `--add-host` entries needs to contain the `Shortname:IPAdress` of each spark-worker and should use resolvable shortnames (names exist in /etc/hosts, dns A records, etc.)


### Configuration

Edit scripts/config/config.yaml and fill out the master field with the IP address of the endpoint you ran the master container. The port must be defined as 7077.

```
master: "spark://178.33.63.238:7077"
```

## How to submit a job to the cluster

As you'll notice the python virtualenv should not the needed to submit the jobs since all the magic will happen inside the docker container.

* Specify the script name using the -s argument.
* Specify the RING nale using the -r argument. 

``` 
[root@node01 ~]# cd /root/spark/scripts/
[root@node01 scripts]# python submit.py -s S3_FSCK/s3_fsck_p0.py -r DATA
```

:warning: **Submit the jobs exactly as shown above. Changes such as adding a ./ to submit.py (ie. python ./submit.py) or
            variables in the script name (ie. S3_FSCK/s3_fsck_${step}.py) can cause loading errors!**

# Single local spark Deployment

## Requirements

* The RING should be in a stable state prior running all of this meaning all the nodes/servers should be present up and running.

We do recommend to run the local instance on the supervisor and adjust accordingly the configuration settings.

* Memory/cores requirements

The more memory/cores you have the faster it is to process the MapReduce. There should be one `spark.executor.instance` per 
spark-worker container or some of your workers will remain idle. If you run 6 spark workers, and one of them also runs the 
master, you can configure 11 `spark.executor.instances`. 5 spark-workers will get 2 instances, the 6th which runs the 
spark-master container will only get 1 executor instance. 

Please adjust it accordingly into the config/config.yml file.

```
spark.executor.cores: 2
spark.executor.instances: 2
spark.executor.memory: "6g"
spark.driver.memory: "6g"
spark.memory.offHeap.enabled: True
spark.memory.offHeap.size: "4g"
```

### Clone the spark script repository
```
[root@node01 ~]# git clone git@github.com:scality/spark.git
```

### Or Download the latest tarball
```
https://bitbucket.org/scality/spark/downloads/
```

### Links to spark scripts Documentation

#### [Some Scripts](scripts/README.md)

#### [Check Orphan/Removal](scripts/orphan/README.md)

#### [SOFS file-system consistency check](scripts/FSCK/README.md)



## Requirements

* The ARC protection level must be defined in `config/config.yml` file

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
# python submit.py -s listkey.py -r DATA
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


--> EDIT BEN MORGE : WE WILL USE THE S3UTILS verifyBucketSproxydKeys SCRIPT (MODIFICATIONS BROUGHT IN S3C-5544) TO LIST THE CONTENT OF THE BUCKETS --> OUTPUT INPROVED, DOC TO FOLLOW



*Simplest way*: dump all S3 keys locally and upload them to the S3 bucket.

```
# docker run --net=host patrickdos/report-sproxyd-keys:basic --debug -s <start_date> -e <end_date> http://127.0.0.1:9000 > /var/tmp/s3_keys.txt
# aws s3 cp /var/tmp/s3_keys.txt s3://$(spark_dir_path)/<RING_NAME>/s3-bucketd/keys.txt
```

* Do not forget the ``--debug`` option. Otherwise it won't print anything.

* ``<start_date>`` and ``end_date`` allow you to give the the oldest and most recent creation date of the S3 objects. Make sure to include all S3 objects available.

With a bucket name of 10 characters or less every object will produce a line of output of around 100 bytes or so (depends
on the length of the nodes IP Address, bucket name and how long the object name is). 


```
aws s3api list-objects --bucket <BUCKET_NAME> --output json --query "[length(Contents[])]"
```

### Translate the ARC S3 keys to RING keys

```
# python submit.py -s S3_FSCK/s3_fsck_p0.py -r DATA
```

#### Make sure a driver ARC is properly configured for srebuildd

If srebuildd is not well configured or has an old configuration the current `config/config.template` file may need the srebuildd URLs fixed.


### Filter the listkeys to return only the SPROXY main_chunk + sproxyd single keys

This step is the longest. For each sproxyd key listed in ``<RING_NAME>/s3-bucketd/``, spark workers
send a ``HEAD`` request to their local srebuildd. Each spark worker scans 2,7M keys a day with spark.executor.cores=3
and spark.executor.instances=6


```
# python submit.py -s S3_FSCK/s3_fsck_p1.py -r DATA
```

This step creates the ``<RING_NAME>/s3fsck/arc-keyss3objects-missing.csv/`` prefix.

### Return all the RING keys that are not indexed by S3

```
# python submit.py -s S3_FSCK/s3_fsck_p2.py -r DATA
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
# python submit.py -s S3_FSCK/s3_fsck_p4.py -r DATA
```

## Output / Storage

All files are written to a unified path in SOFS or S3 Buckets. The config.yml file defines the 
PATH and the RING variables. All output from the below scripts will be found inside /${PATH}/${RING}.

![Script Workflow Expected Output](https://raw.githubusercontent.com/scality/spark/master/scripts/S3_FSCK/workflow_diagram.png?token=AEIJKPZVO3EOGBCLLIGENPDBMHLWA)
