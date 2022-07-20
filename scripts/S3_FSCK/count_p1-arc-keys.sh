#!/bin/bash

TOTAL=0
NUM_HEADERS=$(ls part* | wc -l)
LINES=$(cat part* | wc -l)
let TOTAL+=${LINES}-${NUM_HEADERS}
echo "$TOTAL arc keys parsed from arc-keys.csv"