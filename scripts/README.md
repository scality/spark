# Somes Scripts

## Prerequisites

### Modify the config.yml and set the credentials
```
#cd /root/
#cp spark/scripts/config/config-template.yml spark/scripts/config/config.yml
```

### Edit the spark config

#### Set the correct internal SUP password and well as the supervisor IP if the script is entented to run on a remote location
```
sup:
 url: "https://127.0.0.1:2443"
 login: "root"
 password: ""
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

## Count the number of keys per flag
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

## Count the number of uniq keys per flag

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
