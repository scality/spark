
# Clustering Deployment based on Docker

The documentation will be provided soon

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

### Links to spark scritps

#### [Some Scritps](scripts/README.md)

#### [Check Orphan/Removal](scripts/orphan/README.md)

#### [SOFS file-system consistency check](scripts/FSCK/README.md)


