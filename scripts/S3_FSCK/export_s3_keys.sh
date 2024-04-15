#!/bin/bash

RID=$1
export WORKDIR=/var/tmp/bucketSproxydKeys

if ! [ -d "${WORKDIR}" ]
then
    mkdir -pv ${WORKDIR}
fi

for bucket in $(curl --silent http://localhost:9000/_/raft_sessions/${RID}/bucket | jq -r '.[] | select (. | contains("mpuShadowBucket") | not) | select (. | contains("users..bucket") | not)')

do
    echo "--- Starting on ${bucket} ---"
    docker run \
        --rm \
        -it \
        --net host \
        --entrypoint /usr/local/bin/node \
        -e 'BUCKETD_HOSTPORT=127.0.0.1:9000' \
        -e "BUCKETS=${bucket}" \
        -e 'NO_MISSING_KEY_CHECK=1' \
        -e 'VERBOSE=1' \
        ghcr.io/scality/s3utils:1.14.6 \
        verifyBucketSproxydKeys.js  \
        > ${WORKDIR}/raw_${bucket}_keys.txt

    echo "--- Processing output... ---"
    jq -r '. | select(.message | contains("sproxyd key"))  + {"bucket": .objectUrl  } | .bucket |= sub("s3://(?<bname>.*)/.*"; .bname) | .objectUrl |= sub("s3://.*/(?<oname>.*)$"; .oname) | [.bucket, .objectUrl, .sproxydKey] | @csv' ${WORKDIR}/raw_${bucket}_keys.txt > ${WORKDIR}/${bucket}_keys.txt
    rm -f ${WORKDIR}/raw_${bucket}_keys.txt
    echo
done
