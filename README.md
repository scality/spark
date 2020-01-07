
# Clustering Deployment based on Docker

The documentation will be provided soon

# Single local spark Deployment

## Requirements
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

### Modify the config.yml and set the correct creds/IPs
```
#cd /root/
#cp spark/scripts/config/config-template.yml spark/scripts/config/config.yml
```

### Edit the config

#### Set the correct internal SUP password 
```
sup:
 url: "https://127.0.0.1:2443"
 login: "root"
 password: ""
```

### And set a srebuildd IP
```
  srebuildd_ip: "127.0.0.1"
```

### Create the local directory to save the listkeys/script outputs
```
#mkdir -p /fs/spark/
```

### Run a listkeys against the DATA RING
```
#/root/spark_env/bin/python /root/spark/scripts/listkey.py DATA
```

### Check if all the listkeys are there
```
#ls /fs/spark/listkeys-DATA.csv/*
```

### Run the check orphan script
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/check_orphan.py DATA
```

### Print the list of potential orphans Keys
```
#cat /fs/output/output-spark-ARCORPHAN-DATA.csv/*
```

### Report the list of real corrupted ARC chunks
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/check_orphan_corrupted.py DATA
```

### Print the list of corrupted orphans Keys
```
#cat /fs/output/output-spark-ARCORPHAN-CORRUPTED-DATA.csv/*
```

### Edit the srebuildd config file accordingly or keep 
### Add a chord driver needed to clean the ARC orphan keys
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