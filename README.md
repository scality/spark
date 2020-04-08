
# Clustering Deployment based on Docker

## Requirements

Pull the docker spark-worker image on the servers you want to act as a spark node.

```
#docker pull patrickdos/spark-worker
```

Pull the docker spark-master image on a server ( could be a spark node ).

```
#docker pull patrickdos/spark-master
```


i.e of worker starting script for 6 nodes:
/ring/fs/spark/ should be a distributed file-system since the nodes should have access to the same DATA. ( could be a NFS or a SOFS connector ).
/var/tmp should be a location with at least 10GB.
For the moment you have to set the IPs manually.
So you have to edit it and put the IPs accordingly.

Warning:
	If you choose SOFS, TACO is mandatory, otherwise, will fail when it will create the path to output the results.

```
docker run -d --rm -it  --net=host --name spark-worker --hostname spark-worker  --add-host spark-master:178.33.63.238 --add-host spark-worker:178.33.63.238  --add-host node1:178.33.63.238 \
           --add-host=node01:178.33.63.238  \
           --add-host=node02:178.33.63.219 \
           --add-host=node03:178.33.63.192 \
           --add-host=node04:178.33.63.213 \
           --add-host=node05:178.33.63.77 \
           --add-host=node06:178.33.63.220 \
           -v /ring/fs/spark/:/fs/spark \
           -v /var/tmp:/tmp \
spark-worker
```

## Configuration

Edit scripts/config/config.yaml and fill out the master field accordingly.

```
master: "spark://178.33.63.238:7077"
```

## How to submit a job to the cluster

As you'll notice the python virtualenv should not the needed to submit the jobs since all the magic will happen inside the docker.
* Specify the script name using the -s argument.
* Specify the RING nale using the -r argument. 

``` 
#cd /root/spark/scripts/
#python submit.py -s FSCK/check_volume.py -r META
```


# Single local spark Deployment

## Requirements

* The RING should be in a stable state prior running all of this meaning all the nodes/servers should be present up and running.

We do recommend to run the local instance on the supervisor and adjust accordingly the configuration settings.

* Memory/cores requirements

The more memory/cores you have the faster it is to process the MapReduce but the following should be safe.
Please adjust it accordingly into the config/config.yml file.

```
spark.executor.cores: 2
spark.executor.instances: 2
spark.executor.memory: "6g"
spark.driver.memory: "6g"
spark.memory.offHeap.enabled: True
spark.memory.offHeap.size: "4g"
```

* Disk capacity requirements
90 bytes per key.

eg:
```
ring> supervisor dsoStorage IT
Storage stats:
 Disks: 46
 Objects: 261622847
```

For **261622847** keys it takes:
```
261622847*90 = 23546056230bytes ~ 23546056230/1024 = 22994195 = 23546056230/1024/1024/1024 ~ 21GB
```

```
[root@node01 spark]# du  /fs/spark/listkeys-IT.csv/
22388738	/fs/spark/listkeys-IT.csv/
```
```
[root@node01 spark]# du  /fs/spark/listkeys-IT.csv/ -sh
22G	/fs/spark/listkeys-IT.csv/
```


## Deploy the Spark Virtual env
**http://packages.scality.com/extras/centos/7Server/x86_64/scality/spark_env.tgz**


### Untar it into any directory
```
#cd /root 
#tar xzf spark_env.tgz
```

### Active the virtual env
```
#source spark_env/bin/activate 
```

### Clone the spark script repository
```
#git clone http://bitbucket.org/scality/spark
```

### Or Download the latest tarball
```
https://bitbucket.org/scality/spark/downloads/
```

### Links to spark scripts Documentation

#### [Some Scripts](scripts/README.md)

#### [Check Orphan/Removal](scripts/orphan/README.md)

#### [SOFS file-system consistency check](scripts/FSCK/README.md)


