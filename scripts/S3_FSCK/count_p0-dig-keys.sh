#!/bin/bash

TOTAL=0
SINGLE=$(awk 'BEGIN {count=0} ; /SINGLE/ {count++} END {print count}' part* )
SPLIT=$(awk 'BEGIN {count=0} ;  !/subkey/ && !/SINGLE/ && !seen[$1]++ {count++} END {print count}' part* )
(( TOTAL+=SINGLE+SPLIT ))
echo "${TOTAL} s3 sproxyd dig keys from p0 output"
