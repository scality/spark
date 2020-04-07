# SOFS file system check 


### Run a full listkeys of the customer DATA RING
```
#/root/spark_env/bin/python /root/spark/scripts/listkey.py DATA
```

## Mirroring the META RING

## Copy "live" of the META RING

Ideally you want to do it as fast as possible and all the bizs at once to avoid any inconsistency.
I'd recommend to ask the customer to stop the traffic for at most 5 mins if possible.
Otherwise , make sure to stop the relocation as well as the purge before copying the files.
In the long term we may implement a distributed-lock-mechanism to make sure the copy is consistent accross the board.

```
#salt -G 'roles:ROLE_STORE' cmd.run "for ssd in $(biziodisks | grep ssd); do find /scality/\$ssd/META -type f | tar czf /scality/\$ssd/\$(hostname)-\$ssd.tar -T  - & done"
```

###Duplicate the META RING 

Untar the bizobj to a location where there's enough space.
I'd recommend untaring it into a SOFS dedicated volume since the processing will require a lot of disk space.
Ideally the volume is accessible from all the nodes ( using TACO ) so you can move the tar to the SOFS volume.
This loop could be used to avoid untar the empty directories.

```
#salt -G 'roles:ROLE_STORE' cmd.run "mkdir /ring/fs/Procedures/\$(hostname)"
#salt -G 'roles:ROLE_STORE' cmd.run "mv /scality/ssd*/*tar /ring/fs/Procedures/\$(hostname)"
#salt -G 'roles:ROLE_STORE' cmd.run "for path in $(ls /ring/fs/Procedures/\$(hostname)/*tar*); do  echo mkdir $path ; done"
#salt -G 'roles:ROLE_STORE' cmd.run "cd /ring/fs/Processing/\$(hostname) && for path in $(ls *tar*); do tar xvf \$path -C $path; done"
```


###Script to create the new bizobj start command

```
find /ring/fs/Procedures/$(hostname) -type d -name "ssd*"  |  sed 's/\(.*\)/biziod -N HI -P "bp=\1"/' > /tmp/l
rm  /ring/fs/Procedures/start-final-2-$(hostname).sh

input="/tmp/l"
while read -r line
do
  # do something on $line
  echo "$line" |  sed 's/HI/'$(echo $RANDOM)'/g' >> /ring/fs/Procedures/start-final-2-$(hostname).sh
done < $input
```

Start the bizobjs on all the nodes

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/start-final-2-\$(hostname).sh"
``` 

###Create the bizobj conf files

create-biz-dir.sh
```
cat /ring/fs/Procedures/start-final-2-$(hostname).sh | awk '{print $3}'| xargs -I {} echo "echo "dirsync=0" >  /etc/scality/biziod/bizobj.META.{}" | sh
```

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/create-biz-dir.sh"
```

###Create a new biziod to migrate the objects to 

start-biz.sh
```
mkdir /scality/ssd1/TEMP
biziod -P "bp=/scality/ssd1/TEMP" -N TEMP
bizioopen -c -N TEMP bizobj://META:0
```

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/start-biz.sh"
```

###Generate a list of files to be migrated

mig.sh
```
for i in $(cat /ring/fs/Procedures/start-final-2-$(hostname).sh  | awk '{print $3}');
do
	biziols -N $i bizobj://META:0 | xargs  -P 20 -n 1 -I {}  echo "
	biziocat -N $i bizobj://META:0/{} > /tmp/{};
	biziocatusermd -N $i bizobj://META:0/{} > /tmp/{}.usermd;
	biziocatmd -N $i bizobj://META:0/{} > /tmp/{}.md;
	bizioput -f /tmp/{} -N TEMP bizobj://META:0/{};
	bizioputusermd -f /tmp/{}.usermd -N TEMP bizobj://META:0/{};
	bizioputmd -f /tmp/{}.md -N TEMP bizobj://META:0/{};
	\rm /tmp/{};
	\rm  /tmp/{}.usermd;
	\rm  /tmp/{}.md" | sed 's/\(bizioput.*META:0\)\/\(.*\)\,.*/\1\/\2,00000001/g'
done  > /ring/fs/Procedures/list-$(hostname).txt
```
```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/mig.sh"
```

###Run the migration

run-mig.sh
```
for i in $(ls /ring/fs/Procedures/$(hostname)); do cat /ring/fs/Procedures/$(hostname)/$i | sh  & done
```

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/run-mig.sh"
```


### Copy a node conf

```cp /etc/scality/node-1 /etc/scality/node-rtl```

Modify the ports and the path accordingly into 
/etc/scality/node-rtl/confdb/config.xml

```
      <name>chordringport</name>
      <text>4044</text>
--
      <name>portbind</name>
      <text>4084</text>
--
      <name>portbind</name>
      <text>4444</text>
```


Modify the log path

```
/etc/scality/node-rtl/confdb/config.xml
      <name>logsdir</name>
      <text>/var/log/scality-node-rtl</text>
--
      <name>privatekey</name>
      <text>/etc/scality/node-rtl/security/httpadmin/server.pem</text>
--
      <name>certificatechain</name>
      <text>/etc/scality/node-rtl/security/httpadmin/server.pem</text>
--
      <name>verifylocation</name>
      <text>/etc/scality/node-rtl/security/httpadmin/root.pem</text>
--
      <name>dhparams</name>
      <text>/etc/scality/node-rtl/security/httpadmin/dh1024.pem</text>
```

Modify the instanceID to 1:

```
# /etc/scality/node-rtl/confdb/msgstorenodecfg.xml
      <name>instanceid</name>
      <text>1</text>
```

Modify the IP
```
# /etc/scality/node-rtl/confdb/msgstorenodecfg.xml
      <name>ip</name>
      <text>178.33.63.238</text>
```

Add the biziod to the list of disk
# /etc/scality/node-rtl/confdb/msgstorenodecfg.xml
```
      <name>bizionames</name>
      <val>
        <name>bizioname</name>
        <text>RTL2</text>
      </val>
    </branch>
```

Set the correct META Dsoname
```
#/etc/scality/node-rtl/confdb/msgstorenodecfg.xml
      <name>dsoname</name>
      <text>META</text>
```

Start the new bizstorenode on all the nodes

```
#salt -G 'roles:ROLE_STORE' cmd.run "/usr/bin/bizstorenode -p /var/run/scality/node/bizstorenode-rtl.pid -m -b -c /etc/scality/node-rtl"
```


###Join the nodes to the new META RING  

### Run a full listkeys of the new META RING
```
#/root/spark_env/bin/python /root/spark/scripts/listkey.py META
```

### Make sure all the listkeys are present ( you should get as many files as ring nodes)
```
#ls /ring/fs/spark/listkeys-DATA.csv/*
#ls /ring/fs/spark/listkeys-META.csv/*
```


### Figure out the volumes ID

```
#/root/spark_env/bin/python /root/spark/scripts/FSCK/check_volume.py META
```

```
+-----+------+----------------------------------------+
|devID|hex   |key                                     |
+-----+------+----------------------------------------+
|1    |000001|05FB8F0000000000000000000000010500000040|
|2    |000002|622CFE0000000000000000000000020500000040|
|3    |000003|01CF970000000000000000000000030500000040|
+-----+------+----------------------------------------+
```

### Create an sfused config with all the dev volumes 

The META ring driver should point to the new META RING.
The DATA ring driver should point to a local ring driver to avoid any retries and responds 404 directly.

ie with the volume2
```
#cat /etc/scality/sfused-rtl2-2.conf
{
    "cache:0": {
        "ring_driver": 0,
        "size": 2000000,
        "type": "write_through"
    },
    "cache:1": {
        "ring_driver": 0,
        "size": 10000000,
        "type": "write_through"
    },
    "cache:2": {
        "ring_driver": 1,
        "size": 1000000,
        "type": "write_through"
    },
    "cache:3": {
        "ring_driver": 0,
        "size": 100000,
        "type": "write_through"
    },
    "dlm": {
        "enable": true,
        "rpc_address_selector": "178.33.63.0/24"
    },
    "general": {
        "acl": true,
        "cache_check_time": 86400,
        "cache_enable_checks": true,
        "case_insensitive": true,
        "cat_cos": 4,
        "cat_page_cos": 4,
        "dev": 2,
        "dfree": "1000000000 1000000000",
        "dir_cos": 4,
        "dir_page_cos": 4,
        "dir_update_log_size": 0,
        "file_cos": 2,
        "fs_batch": true,
        "honor_forget": false,
        "inode_cache_size": 2000,
        "max_proc_fd": 40960,
        "n_workers": 50,
        "ring": "IT",
        "rootfs": true,
        "rootfs_cache": 0,
        "rootfs_cos": 4,
        "syslog_facility": "local4"
    },
    "ino_mode:0": {
        "cache": 0,
        "max_file_size": 536870912,
        "type": "mem"
    },
    "ino_mode:2": {
        "cache_md": 3,
        "cache_stripes": 2,
        "fsid": 1,
        "global_dirty_limit": 2000000000,
        "main_cos": 4,
        "max_data_in_main": 0,
        "page_cos": 4,
        "parallel_stripe_deletes": 5,
        "pattern": ".*",
        "pattern_full_path": false,
        "read_ahead": "0",
        "st_uwrite_threshold": "-1",
        "stripe_cos": 2,
        "stripe_size": "4Mi",
        "type": "sparse",
        "update_log_max_size": 0,
        "workers_commit": 4,
        "workers_io": 12
    },
    "ino_mode:3": {
        "cache": 1,
        "type": "mem"
    },
    "quota": {
        "enable": true
    },
    "recycle": {
        "alternative_format": "%Y%m%d-%H%M%S",
        "enable": true,
        "privileged": true,
        "root_name": ".recycle",
        "versioning": false
    },
    "ring_driver:0": {
        "bstraplist": "178.33.63.238:4044",
        "ring": "MDIT",
        "type": "chord"
    },
    "ring_driver:1": {
	"queue_path": "/tmp/RTL2-2",
        "type": "local"
    },
    "transport": {
        "attr_valid_timeout": 86400,
        "big_writes": true,
        "dlm_cache_invalidate_delay_ms": 10000,
        "entry_valid_timeout": 86400,
        "max_tasks": 100,
        "mountpoint": "/ring/rtl2-2",
        "type": "fuse"
    }
}
```


### Bonus track
You can start sfused with the config.
```
#sfused -c /etc/scality/sfused-rtl2-2.conf -n rtl2-2
```

As a result you'll be able to browse the filesystem and even run diagnoses on directories.

ie:
```
 scalfsdiagnose /ring/rtl2-2/HiRes/0/1 -type d
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/4
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/6
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/7
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/8
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/9
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/2
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/5
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/1
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/3
getfattr: Suppression des « / » en tête des chemins absolus
SUCCESS: /ring/rtl2-2/HiRes/0/1/0
```


A diagnose on files would return 404 for the stripes , however, you'll be able to check the META part of the SPARSE FILE.
```
[root@node01 scripts]# scalfsdiagnose -v /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
getfattr: Suppression des « / » en tête des chemins absolus
FAILURE: /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
getfattr: Suppression des « / » en tête des chemins absolus
\_ ino(10878408841717100330)
  \_ chord(2FF3AC96F7DF348C042F2A000000000801000040)
  |- info: mode=100770 nlink=1 uid=1148401 gid=1100513 size=10727954432
  \_ sparse
    \_ db(96F7DF348C042F2A)
      |- info: main(3776)
      \_ index(mono,i)
        |- info: n_in_keys=1558 n_lf_keys=654 pagesize=32768 min=32768 max=32768
        \_ page(34947150C134714:46)
          \_ chord(1716D545CCC71C41FE0261AA6A9BDB0801000020)
          \_ page(A494715618EEF1B:46)
            \_ chord(B0801327CE2CDB52760E76B77F590B0801000020)
            \_ stripe(1278)
              \_ local(26125F96F7DF348C042F2A10AAAB3C0801D65520)
                |- error: failed with SCAL_RING_ENOENT
            \_ stripe(1277)
              \_ local(7F959696F7DF348C042F2AFA520B940801027F20)
                |- error: failed with SCAL_RING_ENOENT
            \_ stripe(1276)
              \_ local(4CF5A596F7DF348C042F2A42F2B3BD0801D6CE20)
                |- error: failed with SCAL_RING_ENOENT
```

### Start the docker SOFS 
This docker is a micro-service that will be in charge of parsing the SPARSE b+tree to report the stripes.
Pass the previous sfused config file to the docker volume parameter.
```
#docker run -d  -v /ring/fs/RTL2/sfused.conf:/home/website/bin/sfused.conf:ro --rm  --net=host --name "sofs" sofswebservice
```

### Make sure the docker works
ie.

```
#ls -li /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
10878408841717100330 -rwxrwx---+ 1 1148401 1100513 10727954432  2 avril  2015 /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
```
```
#curl http://0:9999/sparse/10878408841717100330?option=inode
```

```
#curl http://0:9999/sparse/10878408841717100330?option=inode\&full=True
"oid: 96F7DF348C042F2A cos 4 ver 3776 key 2FF3AC96F7DF348C042F2A000000000801000040stripe size: 8388608stripe count: 1279stripe cos: 2file size: 10727954432md size: 1
```


### Report the list of stripes for all the sparse files

```
#/root/spark_env/bin/python /root/spark/scripts/FSCK/check_files.py META
```


### Generate the dig of all stripes

```
#/root/spark_env/bin/python /root/spark/scripts/FSCK/check_files_p1.py META
```

### Compare the dig to the listkeys to figure out if the files are in a good shape

```
#/root/spark_env/bin/python /root/spark/scripts/FSCK/check_files_p2.py META
```
