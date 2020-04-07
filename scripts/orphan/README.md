# Orphans Check/Removal

### Modify the config.yml and set the credentials
```
#cd /root/
#cp spark/scripts/config/config-template.yml spark/scripts/config/config.yml
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


### Edit the spark config

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


### Report the list of real corrupted ARC keys extra check for the key 70
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/check_orphan_corrupted_single.py DATA
```

### Print the list of corrupted orphan Keys
```
#cat /fs/output/output-spark-ARCORPHAN-CORRUPTED-BUT-OK-LAST-DATA.csv/*
```

### Remove the orphans **Don't run it just yet I still need to put some safe-guard before**
```
#/root/spark_env/bin/python /root/spark/scripts/orphan/remove_orphans.py DATA
```

