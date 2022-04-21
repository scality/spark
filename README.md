
# Clustering Deployment based on Docker

The spark tool can be used to provide a distributed cleanup for application 
level orphans (SOFS and S3 connectors) as well as find and remove ring orphans. 

- For details on SOFS application orphan cleanup `scripts/SOFS_FSCK/README.md`
- For details on S3 application orphan cleanup see `scripts/S3_FSCK/README.md`
- For details on RING orphan cleanup see `scripts/orphan/README.md` 

## Requirements

Pull the docker spark-worker image on the servers you want to act as a spark node.

```
[root@node01 ~]# docker pull patrickdos/spark-worker
```

Pull the docker spark-master image on a server ( could be a spark node ).

```
[root@node01 ~]# docker pull patrickdos/spark-master
```

### Starting the spark cluster
Warning:
If you choose SOFS, TACO is mandatory, otherwise, will fail when it will create the path to output the results.

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
           -v /ring/fs/spark/:/fs/spark \
           -v /var/tmp:/tmp \
            patrickdos/spark-worker
```

* The -v volume mappings are only required if using SOFS to store output not S3. 
  * /ring/fs/spark/ should be a distributed file-system since the nodes should have access to the same DATA. ( could be a NFS or a SOFS connector ).
* /var/tmp should be a location with at least 10GB.
* spark-worker should be the local IP of the node running the container.
* The add-host command should use resolvable shortnames (names exist in /etc/hosts, dns A records, etc.)
* For the moment you have to set the IPs manually.

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
           patrickdos/spark-master
```

## Configuration

Edit scripts/config/config.yaml and fill out the master field accordingly.

```
master: "spark://178.33.63.238:7077"
```

## How to submit a job to the cluster

As you'll notice the python virtualenv should not the needed to submit the jobs since all the magic will happen inside the docker container.

* Specify the script name using the -s argument.
* Specify the RING nale using the -r argument. 

``` 
[root@node01 ~]# cd /root/spark/scripts/
[root@node01 scripts]# python submit.py -s SOFS_FSCK/check_volume.py -r META
```

:warning: **Submit the jobs exactly as shown above. Changes such as adding a ./ to submit.py (ie. python ./submit.py) or
            variables in the script name (ie. S3_FSCK/s3_fsck_${step}.py) can cause loading errors!**

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
* 
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
[root@node01 spark]# du /fs/spark/listkeys-IT.csv/
22388738	/fs/spark/listkeys-IT.csv/
```
```
[root@node01 spark]# du -sh /fs/spark/listkeys-IT.csv/ 
22G	/fs/spark/listkeys-IT.csv/
```


## Centos 7 Installation:Deploy the Spark Virtual env
**http://packages.scality.com/extras/centos/7Server/x86_64/scality/spark_env.tgz**


### Untar it into any directory
``` 
[root@node01 tmp]# cd /root/
[root@node01 ~]# tar xzf spark_env.tgz
```


## Centos 6 installation:Deploy the Spark Virtual env

### Download python2.7 + Centos6 spark_env


**http://sreport.scality.com/video/python-2.7-centos6.tgz**

**http://sreport.scality.com/video/spark_env-centos-6.tgz**


###  Untar the env

```
[root@node01 tmp]# cd /root/
[root@node01 ~]# tar xvzf spark_env-centos-6.tgz
```

### Untar the python2.7 libs

```
[root@node01 ~]# cd /
[root@node01 /]# tar cvzf /root/python-2.7-centos6.tgz
```

### Create the following file

```
[root@node01 ~]# cat /etc/ld.so.conf.d/python27.conf
/usr/local/lib
```

### load the lib

```
[root@node01 ~]# ldconfig
```

## Update Java to version 1.8

```
[root@node01 ~]# yum -y install java-1.8.0-openjdk
```

## Enable the virt_env + Download the spark scripts 

### Active the virtual env
```
[root@node01 ~]# source spark_env/bin/activate 
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


