#!/bin/bash

if [[ $# -ne 2 ]]
then
    echo "Usage: $0 <ring> <bucket>"
    echo "Example: $0 DATA scality-spark-cleanup"
    exit 1
fi
RING=$1
BUCKET=$2

h=$(hostname)
for NODE in ${RING}-${h}-n{1..6}
do
    file_name="listkeys-${NODE}.csv"
    echo "* Dumping all keys from ${NODE} into ${file_name} file"
    # shellcheck disable=SC2312
    ringsh -r "${RING}" -u "${NODE}" node listKeys | awk -F, 'BEGIN{OFS=","}; !/^Use/ {print $1,$2,$3,$4}' > "${file_name}"
done

PS3="Do you want the files automatically uploaded: "
select yn in "Yes" "No"
do
    case "${yn}" in
        Yes)
            echo "* Uploading files to s3://${BUCKET}/${RING}/listkeys.csv"
            for NODE in "${RING}-${h}"-n{1..6}
            do
                file_name="listkeys-${NODE}.csv"
                echo "** Uploading ${file_name}"
                aws --endpoint http://127.0.0.1:8000 s3 cp "${file_name}" s3://"${BUCKET}/${RING}"/listkeys.csv/
            done
            break;;
       No)
            echo "WARNING: Add a reminder task to cleanup the files later to avoid prematurely filling the OS disk"
            break;;
       *)
            echo "Invalid option"
            exit 1
    esac
done

PS3="Do you want the files automatically removed: "
select yn in "Yes" "No"
do
    case "${yn}" in
        Yes)
            for NODE in "${RING}-${h}"-n{1..6}
            do
                rm -f listkeys-"${NODE}".csv
            done
            break;;
        No)
            echo "WARNING: Add a reminder task to cleanup the files later to avoid prematurely filling the OS disk"
            break;;
        *)
            echo "Invalid option"
            exit 1
    esac
done
