
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
