#!/bin/bash
RING=$1
BUCKET=$2

h=$(hostname)
for NODE in ${RING}-${h:4}-n{1..6}
do
    echo "* Dumping all keys from ${NODE} into listkeys-${NODE}.csv file"
    ringsh -r ${RING} -u ${NODE} node listKeys | egrep -v '^Use' | awk  -F, 'BEGIN{OFS=","} {print $1,$2,$3,$4}' > listkeys-${NODE}.csv
done

PS3="Do you want the files automatically uploaded: "
select yn in "Yes" "No"
do
    case $yn in
      Yes)
        echo "* Uploading files to s3://${BUCKET}/${RING}/listkeys.csv"
        for NODE in ${RING}-${h:4}-n{1..6}
        do
            echo "** Uploading listkeys-${NODE}.csv"
            aws --endpoint http://127.0.0.1:8000 s3 cp listkeys-${NODE}.csv s3://${BUCKET}/${RING}/listkeys.csv/
        done
        break;;
       No)
        echo "WARNING: Add a reminder task to cleanup the files later to avoid prematurely filling the OS disk"
        break;;
    esac
done

PS3="Do you want the files automatically removed: "
  select yn in "Yes" "No"
  do
      case $yn in
        Yes)
          for NODE in ${RING}-${h:4}-n{1..6}
          do
              rm -f listkeys-${NODE}.csv
          done
          break;;
         No)
          echo "WARNING: Add a reminder task to cleanup the files later to avoid prematurely filling the OS disk"
          break;;
      esac
  done
