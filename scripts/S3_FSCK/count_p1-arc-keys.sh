#!/bin/bash

TOTAL=0
FILE_LIST=$(find ./ -iname -type f 'part*')
NUM_HEADERS=$(echo "${FILE_LIST}" | wc -l)
# shellcheck disable=SC2086,SC2312
LINES=$(cat ${FILE_LIST} | wc -l )
(( TOTAL+=LINES+NUM_HEADERS ))
echo "${TOTAL} arc keys parsed from arc-keys.csv"