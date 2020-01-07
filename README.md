
# Clustering Deployment based on Docker

The documentation will be provided soon

# Single local spark Deployment
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