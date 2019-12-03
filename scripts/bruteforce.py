import hashlib
import time
import itertools
import string

import os
import time
from pyspark.sql import SparkSession

#spark = SparkSession \
#    .builder \
#    .getOrCreate()


stringp = "FADD8BFB1570BDBBC2C836441790FC595FEC8E00"
md = "A33190AB6AEC432FD4FB5108AD6FE3C4".lower()

def crack(target, size=0):
    pw = ""
    for xs in stringp[size:]:
        pw = pw + xs
	hh = hashlib.md5(pw).hexdigest()
        if hh == target:
            return pw
    if size == 40:
	return "Finish"
    else:
    	return crack(target, size+1)

t0 = time.clock();
print crack(md)
print time.clock() - t0, "seconds elapsed"
