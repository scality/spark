# SOFS file system consistency check 


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

### Duplicate the META RING 

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


### Script to create the new bizobj start command

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

### Create the bizobj conf files

create-biz-dir.sh
```
cat /ring/fs/Procedures/start-final-2-$(hostname).sh | awk '{print $3}'| xargs -I {} echo "echo "dirsync=0" >  /etc/scality/biziod/bizobj.META.{}" | sh
```

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/create-biz-dir.sh"
```

### Create a new biziod to migrate the objects to 

start-biz.sh
```
mkdir /scality/ssd1/TEMP
biziod -P "bp=/scality/ssd1/TEMP" -N TEMP
bizioopen -c -N TEMP bizobj://META:0
```

```
#salt -G 'roles:ROLE_STORE' cmd.run "sh /ring/fs/Procedures/start-biz.sh"
```

### Generate a list of files to be migrated

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

### Run the migration

run-mig.sh
```
cat /ring/fs/Procedures/list-$(hostname).txt | sh
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

```
#/etc/scality/node-rtl/confdb/msgstorenodecfg.xml
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


### Join the nodes to the new META RING  

![picture](img/ring.png)

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
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_volume.py META
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

### Create an sfused config for each volumes ID 

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
#docker run -d  -v /ring/fs/RTL2/sfused.conf:/home/website/bin/sfused.conf:ro --rm  --net=host --name "sofs" patrickdos/sofswebservice
```

### Make sure the docker works
ie.

```
#ls -li /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
10878408841717100330 -rwxrwx---+ 1 1148401 1100513 10727954432  2 avril  2015 /ring/rtl2-2/HiRes/0/1/3/01SK1B8982T11SVD.mxf
```
```
#curl http://0:9999/sparse/10878408841717100330?option=inode
["C48AE496F7DF348C042F2A9716BF8808016BF520", "9DB36E96F7DF348C042F2AB834711B0801D57F20", "45108E96F7DF348C042F2A3E800AAF0801D3CB20", "565DBA96F7DF348C042F2A7507CAEB0801DC5B20", "CC7BB196F7DF348C042F2A28FB26E50801F21520", "D7E5FF96F7DF348C042F2A1E8FE5C308013A1D20", "708F4996F7DF348C042F2A61546EF808019E2420", "1E52FC96F7DF348C042F2A12D8F5AD0801B67B20", "33EBC996F7DF348C042F2AD2A99AC80801086420", "D3E81D96F7DF348C042F2A66AC320A08012A6620", "6C635296F7DF348C042F2AEA8C58EE08013CAA20", "0F7B5696F7DF348C042F2A1E79C03B0801910420", "7AC40996F7DF348C042F2A5FD2C649080124E220", "AF634896F7DF348C042F2A3391AACC0801A8C620", "CC81A196F7DF348C042F2A0FAFAFA30801B5E020", "40308E96F7DF348C042F2AE3EB2158080182C120", "C0218396F7DF348C042F2A6F6A5B85080113D920", "CC7C6596F7DF348C042F2A477886C00801523620", "83DD4396F7DF348C042F2A77AB56BA0801CC8820", "3D6F0596F7DF348C042F2A406A0C7F0801215320", "B7227E96F7DF348C042F2A0A4FDDD50801598320", "F5C71196F7DF348C042F2AAD1C513008010F4920", "66D71496F7DF348C042F2A688745DC0801E77920", "ED233C96F7DF348C042F2AE1D0722E0801ACCD20", "198D7A96F7DF348C042F2A505363A30801259520", "C2F55996F7DF348C042F2AB07401610801181020", "9E185796F7DF348C042F2A0609A33D0801B13520", "0D6B4896F7DF348C042F2A04576835080101B720", "D7254F96F7DF348C042F2A70920F69080144A420", "15607796F7DF348C042F2AB77F74320801B41320", "66D88A96F7DF348C042F2A5252F7490801545220", "55608496F7DF348C042F2A9B417DC108019BDA20", "0387B096F7DF348C042F2A6B43A43108019D9120", "33F38796F7DF348C042F2AC6A206740801F7BB20", "F2C3CC96F7DF348C042F2A9AE8642908019E9420", "BED6A096F7DF348C042F2A0B61E2200801B98E20", "00B50696F7DF348C042F2A8096BE500801CA3820", "2235C396F7DF348C042F2A1721E98608016E3220", "A0943C96F7DF348C042F2AF85E9F8A0801C36420", "ADFD0796F7DF348C042F2AB577ED01080170E320", "24C14D96F7DF348C042F2A82DADC4608013BB920", "3393E896F7DF348C042F2A5DAEA3E60801245D20", "868B9796F7DF348C042F2AE4CB630508012A7A20", "D3989F96F7DF348C042F2AC19D2AD608011A7D20", "C7C06F96F7DF348C042F2A49921CF30801931E20", "3FC31C96F7DF348C042F2A9E525F91080137A920", "151C4F96F7DF348C042F2AB3DB1F4E08019D6320", "FD5EC596F7DF348C042F2A34E95CD008017D1220", "9A2EC196F7DF348C042F2AC52D1E040801042B20", "7FDBC996F7DF348C042F2AA0462F4D08016E4020", "EF9B4096F7DF348C042F2AD8208AD20801C8C020", "BF9A5296F7DF348C042F2A519FD1D8080118B520", "D2049
```

```
#curl http://0:9999/sparse/10878408841717100330?option=inode\&full=True
"oid: 96F7DF348C042F2A cos 4 ver 3776 key 2FF3AC96F7DF348C042F2A000000000801000040stripe size: 8388608stripe count: 1279stripe cos: 2file size: 10727954432md size: 1
```


### Filter the ARC keys for the listkeys 
```
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_files_p0.py DATA
```

### Move the result to a new dir so we keep the same path
```
mv /ring/fs/output/output-sparse-ARC-FILES-DATA.csv /ring/fs/output/output-sparse-ARC-FILES-META.csv

```

### Report the list of stripes for all the sparse files

```
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_files_p1.py META
```


### Generate the dig of all stripes

```
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_files_p2.py META
```

### Compare the dig to the listkeys to figure out if the files are in a good shape

```
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_files_p3.py META
```

ie.of outputs:
```
#cat /ring/fs/spark/output/output-sofs-SPARSE-FILE-SHAPE-INODE-META.csv/*
59CB69113EF8D8A2,6470380812439705762,78360959CB69113EF8D8A2000000000801000040
5F55B8BB214B841A,6869599920480551962,516FBB5F55B8BB214B841A000000000801000040
554E4629A9F1A2F2,6146927686166094578,52728C554E4629A9F1A2F2000000000801000040
B54475FA93FDF792,13061694538366449554,3AF3CAB54475FA93FDF792000000000801000040
80C0FCD4276FF5B2,9277693220508136882,C8055E80C0FCD4276FF5B2000000000801000040
F7E9BB0AFB0AC79A,17864015052777637786,D7F556F7E9BB0AFB0AC79A000000000801000040
BF85DD192E6ABF5A,13800679733369421658,0A5EFDBF85DD192E6ABF5A000000000801000040
4C752307292B7802,5509348232820127746,A2CA654C752307292B7802000000000801000040
2F00A3762FA42472,3386886647783367794,0841BE2F00A3762FA42472000000000801000040
2C403013C98B2582,3188601397722162562,00FE2C2C403013C98B2582000000000801000040
083B988FB42EEFCA,593235518900072394,AEA465083B988FB42EEFCA000000000801000040
BFA13DFEF4CEE182,13808386097732837762,3BFB38BFA13DFEF4CEE182000000000801000040
5CB75E1F2C0A43C2,6680912060203287490,DC232C5CB75E1F2C0A43C2000000000801000040
8E5810CB733BFDCA,10256966617334021578,8C58038E5810CB733BFDCA000000000801000040
069BF7860CF7F38A,476246340732973962,CAE950069BF7860CF7F38A000000000801000040
4B291931869E002A,5415887727392325674,8B59874B291931869E002A000000000801000040
0B17BC8517A0A6AA,799314738694629034,71A4A70B17BC8517A0A6AA000000000801000040
C451CB5121871582,14146311453862729090,81B751C451CB5121871582000000000801000040
DD23AE7FCA36204A,15934771770443571274,DE42F5DD23AE7FCA36204A000000000801000040
4E4700422482825A,5640477342385209946,68647E4E4700422482825A000000000801000040
78D43C7ED62CEC8A,8706650495070825610,AE037A78D43C7ED62CEC8A000000000801000040
331873C6C87E2EEA,3681819992979812074,E598D4331873C6C87E2EEA000000000801000040
9F7D3F69EA350E72,11492411548252835442,0362509F7D3F69EA350E72000000000801000040
1FB3289B8BF5E2AA,2284214084540162730,AF7CEA1FB3289B8BF5E2AA000000000801000040
DEFE0A1FF4093E1A,16068291652859018778,13660ADEFE0A1FF4093E1A000000000801000040
7211F2921BE430EA,8219617504274952426,4E028E7211F2921BE430EA000000000801000040
B319DA74C1D13A6A,12905586402209643114,BEFA63B319DA74C1D13A6A000000000801000040
EEF599F5850EF40A,17218838030099346442,F70568EEF599F5850EF40A000000000801000040
905294E2BFA92A5A,10399538191150951002,42E489905294E2BFA92A5A000000000801000040
03CFC2B759A3BB02,274652195031595778,1C767203CFC2B759A3BB02000000000801000040
```


### Return the corrupted paths

As a requirement we need to build a list of inodes with the paths.

```
find /ring/rtl2-2/HiRes/ -printf "%i,%p\n" > /ring/fs/spark/inodes-2.txt 
```

```
#/root/spark_env/bin/python /root/spark/scripts/SOFS_FSCK/check_files_p4.py META
```

i.e of ouputs:
```
cat /ring/fs/spark/output/output-sofs-SPARSE-FILE-SHAPE-INODE-ALL-META.csv/*
12905586402209643114,/ring/rtl2-2/HiRes/6/1/8/01SZ2D0000359WVX.mxf,1
9277693220508136882,/ring/rtl2-2/HiRes/2/6/8/01SP1F4604T11S4K.mxf,1
16068291652859018778,/ring/rtl2-2/HiRes/7/1/2/01SU1B2788T11C1H.mxf,1
17218838030099346442,/ring/rtl2-2/HiRes/6/3/7/01SP1F4963T11S3Q.mxf,1
3681819992979812074,/ring/rtl2-2/HiRes/1/0/1/01SU2HT867T11SVM.mxf,1
799314738694629034,/ring/rtl2-2/HiRes/9/8/1/01SU1E1479T11SVM.mxf,1
476246340732973962,/ring/rtl2-2/HiRes/1/2/5/01SU1E3052T15SVL.mxf,1
6146927686166094578,/ring/rtl2-2/HiRes/6/2/9/01RO2RM009878AVX.mxf,1
5509348232820127746,/ring/rtl2-2/HiRes/0/6/0/01SU2HE712T11S2F.mxf,1
5640477342385209946,/ring/rtl2-2/HiRes/6/5/9/01SE1B2345T11SVY.mxf,1
2284214084540162730,/ring/rtl2-2/HiRes/4/3/1/01SK1K0063T11SV2.mxf,1
6869599920480551962,/ring/rtl2-2/HiRes/0/1/7/01SU1HB241T11SV9.mxf,1
6470380812439705762,/ring/rtl2-2/HiRes/7/7/8/01SU2HS042T11CVC.mxf,1
17864015052777637786,/ring/rtl2-2/HiRes/8/7/9/01SU1HA840T11CVK.mxf,1
15934771770443571274,/ring/rtl2-2/HiRes/9/3/2/01SU2HM817T11WV6.mxf,1
13808386097732837762,/ring/rtl2-2/HiRes/6/5/1/01SU2HI220T22CVL.mxf,1
```
