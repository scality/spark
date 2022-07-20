#!/bin/bash

TOTAL=0
ARC_REPLICA=$(awk -F',' 'BEGIN {count=0} ; !seen[$2]++ && $2 ~ "[A-F0-9]{30}50[A-F0-9]{6}30" && $4 == "0" {count++} END {print count}' *)
ARC_STRIPE=$(awk -F',' 'BEGIN {count=0} ; !seen[$2]++ && $2 ~ "[A-F0-9]{30}51[A-F0-9]{6}70" {count++} END {print count}' *)
let TOTAL+=${ARC_REPLICA}+${ARC_STRIPE}
echo "$TOTAL ring keys dumped from the listkeys.csv"