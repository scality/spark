
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
#git clone http://bitbucket.org/scality-patrick/spark
```

### Modify the config.yml and set the credentials
```
#cd /root/
#cp spark/scripts/config/config-template.yml spark/scripts/config/config.yml
```

### Edit the config

#### Set the correct internal SUP password and well as the supervisor IP if the script is entented to run on a remote location
```
sup:
 url: "https://127.0.0.1:2443"
 login: "root"
 password: ""
```

### Specify any srebuildd IP
```
  srebuildd_ip: "127.0.0.1"
```

### Create the local directory to save the listkeys as well as the outputs for the scripts
```
#mkdir -p /fs/spark/
```

### Run a full listkeys of the DATA RING
```
#/root/spark_env/bin/python /root/spark/scripts/listkey.py DATA
```

### Make sure all the listkeys are present ( you should get as many files as ring nodes)
```
#ls /fs/spark/listkeys-DATA.csv/*
```


# Check/Removal Orphans set of scripts

### Run the check orphan script
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/check_orphan.py DATA
```

### Print the list of potential orphan Keys
```
#cat /fs/output/output-spark-ARCORPHAN-DATA.csv/*
```

### Report the list of real corrupted ARC keys
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/check_orphan_corrupted.py DATA
```

### Print the list of corrupted orphan Keys
```
#cat /fs/output/output-spark-ARCORPHAN-CORRUPTED-DATA.csv/*
```

### Edit the srebuildd config file accordingly 
### Add a chord driver needed to allow the script to delete the ARC orphan keys
#### Copy the ring_driver0 to ring_driver1

```
    },
    "ring_driver:0": {
        "alias": "arcdata",
        "bstraplist": "178.33.63.219:4249",
        "get_reconstruct_buffer_size": "1048576",
        "ring": "DATA",
        "type": "arcdata"
    },
```
```
    },
    "ring_driver:1": {
        "alias": "chord",
        "bstraplist": "178.33.63.219:4249",
        "ring": "DATA",
        "type": "chord"
    },
```

#### Restart the srebuildd connector
```
systemctl restart scality-srebuildd
```

### Remove the orphans **Don't run it just yet I still need to put some safe-guard before**
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/remove_orphans.py DATA
```

# Count the number of keys per flag
```
#/root/spark_env/bin/python /root/spark/scripts/count-flag.py DATA
```
Output:
```
+---+----------+
|_c3|count(_c1)|
+---+----------+
| 16|   4403504|
| 48|       252|
| 32|    177258|
|  0| 253136044|
+---+----------+
```

# Count the number of uniq keys per flag

```
#/root/spark_env/bin/python /root/spark/scripts/count-flag-uniq.py DATA
```

Output:
```
+---+-------------------+
|_c3|count(DISTINCT _c1)|
+---+-------------------+
| 16|            1046962|
| 48|                 84|
| 32|              59085|
|  0|           61031785|
+---+-------------------+
```

# Check the coherence of all the sproxyd/split files.

The idea is making sure all the subpart are present ( DailyMotion use-case ) as such the sproxyd/split file can be retrieved

This may as well be the baseline to allow us to do a full consistency check of all the sfused/S3 objects as well.

We may even call it an ***application-fsck***.

> Documentation will be updated soon.

