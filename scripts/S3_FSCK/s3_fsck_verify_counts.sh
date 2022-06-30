#!/bin/bash

# DEFINE SOME DEFAULT VARAIBLES
count_types=('listkeys.csv' 's3-bucketd' 's3-dig-keys.csv' 'arc-keys.csv')
declare -A type_by_dir
type_by_dir[listkeys.csv]='count_listkeys'
type_by_dir[s3-bucketd]='count_bucketd'
type_by_dir[s3-dig-keys.csv]='count_dig-keys'
type_by_dir[arc-keys.csv]='count_arc-keys'

dir_name=$(basename $(pwd))

containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

select_count_type_by_dir_name () {
  select yn in "Yes" "No"
  do
    case $yn in
      Yes)
        command="${type_by_dir[${1}]}"
        echo "The command is ${command}"
        break;;
      No)
        break;;
    esac
  done
}

select_count_type () {
  #arr=("listkeys.csv (listkeys output)" "s3-bucketd (s3utils output)" "s3-dig-keys.csv (s3_fsck_p0.py output)" "arc-keys.csv (s3_fsck+p1.py output)")
  PS3="What type of output is being counted: "
  select type in "${count_types[@]}"
  do
  if [ "${#count_types[@]}" -ge "$REPLY" ] ; then
    case $item in
      *)
        echo "${type_by_dir[${type}]}"
        break;;
    esac
  fi
  done

}

if containsElement "${dir_name}" "${count_types[@]}" ]] ; then
  PS3="The directory name matches ${dir_name}, would you like to start that type of count: "
  select_count_type_by_dir_name "${dir_name}"
fi
if [ -z "${command}" ] ; then
  command=$(select_count_type)
fi

echo "THE TYPE OF COUNT IS: ${command}"

count_listkeys () {
  echo -e "\n** Starting to parse listkeys **"
  TOTAL=0
  ARC_REPLICA=$(awk -F',' 'BEGIN {count=0}; !seen[$2]++ && $2 ~ "[A-F0-9]{30}50[A-F0-9]{6}30" && $4 == "0" {count++} END {print count}' *); ARC_STRIPE=$(awk -F',' 'BEGIN {count=0}; !seen[$2]++ && $2 ~ "[A-F0-9]{30}51[A-F0-9]{6}70" {count++} END {print count}' *)
  let TOTAL+=${ARC_REPLICA}+${ARC_STRIPE}
  echo -e "** Finished parsing listkeys **\n"
  echo -e "***** Final Tally Results for listkeys (aka listkeys.py or listkeys.sh) *****"
  echo "TOTAL ring keys parsed: ${TOTAL}"

  # echo -e "\** Starting to parse listkeys **"
  # read -r total other total_arc arc_replica arc_stripe file_total_arc file_arc_replica file_arc_stripe file_total <<< "0 0 0 0 0 0 0 0 0"
  # for FILE in $(ls listkey*.csv)
  # do
  #   echo -n "${FILE}: "
  #   AWK_OUTPUT=$(awk -F ',' 'BEGIN{ARCREPLICA=0;ARCSTRIPE=0; TOTAL=0}{if (!seen[$2] && $2 ~ /[A-F0-9]{30}50[A-F0-9]{6}[234569A]0/ && $4 == 0) {ARCREPLICA++; fARCREPLICA++; seen[$2]++} if (!seen[$2] && $2 ~ /[A-F0-9]{30}51[A-F0-9]{6}[78]0/ && $4 == 0) {ARCSTRIPE++; fARCSTRIPE++; seen[$2]++} }END{ print NR, NR-ARCREPLICA-ARCSTRIPE, ARCREPLICA+ARCSTRIPE, ARCREPLICA, ARCSTRIPE}' ${FILE})
  #   read -r file_total file_other file_total_arc file_arc_replica file_arc_stripe <<<$(echo ${AWK_OUTPUT})
  #   echo "Total=${file_total}, OTHER=${file_other}, Total ARC=${file_total_arc}, ARC Replica=${file_arc_replica}, ARC Stripe=${file_arc_stripe}"
  #   total=$((total+file_total))
  #   other=$((other+file_other))
  #   total_arc=$((total_arc+file_total_arc))
  #   arc_replica=$((arc_replica+file_arc_replica))
  #   arc_stripe=$((arc_stripe+file_arc_stripe))
  #   read -r file_total file_other file_total_arc file_arc_replica file_arc_stripe <<< "0 0 0 0 0"
  # done
  #
  # echo -e "** Finished parsing listkeys **\n"
  # echo -e "***** Final Tally Results for listkeys (aka listkeys.py or listkeys.sh) *****"
  # echo "Total=${total}, OTHER=${other}, Total ARC=${total_arc}, ARC Replica=${arc_replica}, ARC Stripe=${arc_stripe}"
}

count_bucketd () {
  echo -e "\n** Starting to parse s3-bucketd keys **"
  TOTAL=0
  tmp_total=0
  for FILE in $(find ./ -maxdepth 1 -type f)
  do
    echo -n "${FILE:2}: "
    tmp_total=$(wc -l "${FILE}" | cut -d' ' -f 1)
    echo "${tmp_total} Lines"
    TOTAL=$((TOTAL+tmp_total))
  done
  echo -e "** Finished parsing s3-bucketd keys**\n"
  echo -e "***** Final Tally Results for s3-bucketd (aka s3utils verifyBucketSproxydKeys.js) *****"
  echo "TOTAL sproxyd dig keys parsed: ${TOTAL}"
}

count_dig-keys () {
  echo -e "\n** Starting to parse s3-dig-keys **"
  TOTAL=0
  SINGLE=$(awk 'BEGIN {count=0}; /SINGLE/ {count++} END {print count}' part*); SPLIT=$(awk 'BEGIN {count=0};  !/subkey/ && !/SINGLE/ && !seen[$1]++ {count++} END {print count}' part*)
  let TOTAL+=${SINGLE}+${SPLIT}
  echo -e "** Finished parsing s3-dig-keys **\n"
  echo -e "***** Final Tally Results for s3-dig-keys (aka s3_fsck_p0.py) *****"
  echo "TOTAL sproxyd dig keys parsed: ${TOTAL}"
}

count_arc-keys () {
  echo -e "\n** Starting to parse arc-keys **"
  TOTAL=0
  NUM_HEADERS=$(ls part* | wc -l)
  LINES=$(cat part* | wc -l)
  let TOTAL+=${LINES}-${NUM_HEADERS}
  echo -e "** Finished parsing arc-keys **\n"
  echo -e "***** Final Tally Results for arc-keys (aka s3_fsck_p1.py) *****"
  echo "TOTAL arc keys parsed: ${TOTAL}"
  # echo -e "\** Starting to parse arc-keys **"
  # read -r total other total_arc arc_replica arc_stripe file_total_arc file_arc_replica file_arc_stripe file_total <<< "0 0 0 0 0 0 0 0 0"
  # for FILE in $(ls part*.csv)
  # do
  #   echo -n "${FILE}: "
  #   AWK_OUTPUT=$(awk -F ',' 'BEGIN{ARCREPLICA=0;ARCSTRIPE=0; TOTAL=0}{if (!seen[$3] && $3 ~ /[A-F0-9]{30}50[A-F0-9]{6}[234569A]0/) {ARCREPLICA++; fARCREPLICA++; seen[$3]++} if (!seen[$3] && $3 ~ /[A-F0-9]{30}51[A-F0-9]{6}[78]0/) {ARCSTRIPE++; fARCSTRIPE++; seen[$2]++} }END{ print NR, NR-ARCREPLICA-ARCSTRIPE, ARCREPLICA+ARCSTRIPE, ARCREPLICA, ARCSTRIPE}' ${FILE})
  #   read -r file_total file_other file_total_arc file_arc_replica file_arc_stripe <<<$(echo ${AWK_OUTPUT})
  #   echo "Total=${file_total}, OTHER=${file_other}, Total ARC=${file_total_arc}, ARC Replica=${file_arc_replica}, ARC Stripe=${file_arc_stripe}"
  #   total=$((total+file_total))
  #   other=$((other+file_other))
  #   total_arc=$((total_arc+file_total_arc))
  #   arc_replica=$((arc_replica+file_arc_replica))
  #   arc_stripe=$((arc_stripe+file_arc_stripe))
  #   read -r file_total file_other file_total_arc file_arc_replica file_arc_stripe <<< "0 0 0 0 0"
  # done
  #
  # echo -e "** Finished parsing arc-keys **\n"
  # echo -e "***** Final Tally Results for arc-keys (aka s3_fsck_p1.py) *****"
  # echo "Total=${total}, OTHER=${other}, Total ARC=${total_arc}, ARC Replica=${arc_replica}, ARC Stripe=${arc_stripe}"
}

${command}
