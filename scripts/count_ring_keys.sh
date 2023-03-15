#!/bin/bash

TOTAL=0
ARC_REPLICA=$(awk -F',' 'BEGIN {count=0} ; !seen[$2]++ && $2 ~ "[A-F0-9]{30}50[A-F0-9]{6}[012345]0" && $4 == "0" {count++} END {print count}' ./*)
ARC_STRIPE=$(awk -F',' 'BEGIN {count=0} ; !seen[$2]++ && $2 ~ "[A-F0-9]{30}5[01][A-F0-9]{6}70" {count++} END {print count}' ./*)
(( TOTAL+=ARC_REPLICA+ARC_STRIPE ))
echo "${TOTAL} ring keys dumped from the listkeys.csv (${ARC_REPLICA}, ${ARC_STRIPE})"
