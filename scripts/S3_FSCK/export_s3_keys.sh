#!/bin/bash

# EXIT CODES
RC_NO_RAFT_ID=10
RC_FILE_PERMISSIONS=11
RC_NO_BUCKETD_FOUND=12
RC_UNKNOWN_PARAMETER=13
RC_ARG_MISSING_VALUE=14
RC_AUTO_DETECT_FAILURE=15
RC_NETWORK_CMD_NOT_FOUND=16
RC_NO_BUCKETS_FOUND_IN_RID=20
RC_EXPORT_S3_KEYS_FAILURE=21
RC_PROCESS_KEYS_FAILURE=22
RC_COUNT_MISMATCH=23
RC_INVALID_S3UTILS_VERSION=24

# GLOBALS
BUCKETD_HOST=''
BUCKETD_PORT=9000
BUCKETS=()
KEEP_RAW_FILES='N'
S3UTILS_IMAGE=registry.scality.com/s3utils/s3utils
S3UTILS_VERSION=''
S3UTILS_MIN_VERSION=1.14.0
UNPROCESSED_BUCKETS=()
UNBOUND_BUCKETD=("[::]:${BUCKETD_PORT}" ":::${BUCKETD_PORT}" "0.0.0.0:${BUCKETD_PORT}")
WORKDIR=/var/tmp/bucketSproxydKeys

# Usage menu
function usage() {
    cat <<-EOF
		Usage: $(basename "$0") -r <RAFT_ID> [-i <IP_ADDRESS>] [-k]

		Spark S3_FSCK Export S3 Keys Script
		-h                    : Optional. Print this help message.
		-i <IP_ADDRESS>       : Optional. The IP address of the bucketd server. Tries to determine the IP address if not provided. Default: <determined by script>
		-k                    : Optional. Keep the raw files after processing. Requires additional disk space. Default: N
		-p <PORT>             : Optional. The port of the bucketd server. Required if the server changes the default bucketd port. Default: 9000
		-r <RAFT_ID>          : Required. The raft ID to export S3 keys for. Example: 1
		-s <S3UTILS_IMAGE>    : Optional. The s3utils docker image to use. Default: registry.scality.com/s3utils/s3utils
		-v <S3UTILS_VERSION>  : Optional. The s3utils docker image version to use. Default: 1.14.0 (also the minimum version required)
		-w <WORKDIR>          : Optional. The working directory to use. Default: /var/tmp/bucketSproxydKeys
		EOF
}

# Parse command line arguments
while getopts ":hi:kp:r:s:v:w:" opt; do
    case ${opt} in
        h )
            usage
            exit 0
            ;;
        i )
            BUCKETD_HOST=${OPTARG}
            ;;
        k )
            KEEP_RAW_FILES="Y"
            ;;
        p )
            BUCKETD_PORT=${OPTARG}
            UNBOUND_BUCKETD=("[::]:${BUCKETD_PORT}" ":::${BUCKETD_PORT}" "0.0.0.0:${BUCKETD_PORT}")
            ;;
        r )
            RAFT_ID=${OPTARG}
            ;;
        s )
            S3UTILS_IMAGE=${OPTARG}
            ;;
        v )
            S3UTILS_VERSION=${OPTARG}
            ;;
        w )
            WORKDIR=${OPTARG}
            ;;
        \? )
            echo "Invalid option: -$OPTARG" 1>&2
            usage
            exit ${RC_UNKNOWN_PARAMETER}
            ;;
        : )
            echo "Option -$OPTARG requires an argument." 1>&2
            usage
            exit ${RC_ARG_MISSING_VALUE}
            ;;
        * )
            usage
            exit ${RC_UNKNOWN_PARAMETER}
            ;;
    esac
done

# Check if a raft id was passed
if [[ -z ${RAFT_ID} ]]; then
    echo -e "Error: A raft ID is required.\n"
    usage
    exit ${RC_NO_RAFT_ID}
fi

if [[ ${KEEP_RAW_FILES} == "Y" ]]; then
    echo "Keeping raw files after processing."
fi

# FUNCTIONS

function check_network_cmd() {
    if command -v netstat >/dev/null 2>&1; then
        NETWORK_CMD="netstat"
    elif command -v ss >/dev/null 2>&1; then
        NETWORK_CMD="ss"
    else
        NETWORK_CMD=''
    fi
}

function is_valid_ip() {
    local ip=$1
    local stat=1

    if [[ ${ip} =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
        OIFS=${IFS}
        IFS='.'
        ip=("${ip}")
        IFS=${OIFS}
        [[ ${ip[0]} -le 255 && ${ip[1]} -le 255 &&
            ${ip[2]} -le 255 && ${ip[3]} -le 255 ]]
        stat=$?
    fi
    return "${stat}"
}

# shellcheck disable=SC2312
function init() {
    check_network_cmd || exit "${RC_NETWORK_CMD_NOT_FOUND}"
    if ! [[ -d ${WORKDIR} ]]; then
        if ! mkdir -pv "${WORKDIR}"; then
            echo "$(date -u +"%FT%TZ") ERROR: Failed to create ${WORKDIR}"
            exit "${RC_FILE_PERMISSIONS}"
        else
            echo "$(date -u +"%FT%TZ") INFO: Created ${WORKDIR}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        fi
    fi
    get_s3utils_versions
}

# shellcheck disable=SC2312
function find_bucketd_instance() {
    if [[ -z ${NETWORK_CMD} ]] && [[ -z ${BUCKETD_HOST} ]]; then
        echo "$(date -u +"%FT%TZ") ERROR: Neither netstat nor ss commands are available for Bucketd port auto detection." | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        exit "${RC_NO_BUCKETD_FOUND}"
    elif [[ -z ${NETWORK_CMD} ]] && [[ -n ${BUCKETD_HOST} ]]; then
        echo "$(date -u +"%FT%TZ") INFO: Using BUCKETD_HOST: ${BUCKETD_HOST} and BUCKETD_PORT: ${BUCKETD_PORT}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        return 0
    elif [[ -n ${NETWORK_CMD} ]] && [[ -z ${BUCKETD_HOST} ]]; then
        echo "$(date -u +"%FT%TZ") INFO: No BUCKETD_HOST provided. Attempting to auto detect." | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        # Capture both IPv4 and IPv6 bucketd instances
        SS_OUTPUT=$(${NETWORK_CMD} -tlnp | awk "\$4 ~ /^(([0-9]{1,3}\.){3}[0-9]{1,3}|\[::\]|::):${BUCKETD_PORT}$/ {print \$4}")
        # If the SS_OUTPUT is equal to the UNBOUND_BUCKETD string then set BUCKETD_HOST to 127.0.0.1
        if [[ ${SS_OUTPUT} == "${UNBOUND_BUCKETD[0]}" ]] || [[ ${SS_OUTPUT} == "${UNBOUND_BUCKETD[1]}" ]] || [[ ${SS_OUTPUT} == "${UNBOUND_BUCKETD[2]}" ]]; then
            BUCKETD_HOST="127.0.0.1"
            echo "$(date -u +"%FT%TZ") INFO: Found bucketd instance matched UNBOUND_BUCKET. Using BUCKETD_HOST: ${BUCKETD_HOST}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        else
            BUCKETD_HOST=$(echo "${SS_OUTPUT}" | cut -d: -f1)
            if [[ -z ${BUCKETD_HOST} ]] || [[ $(is_valid_ip "${BUCKETD_HOST}") == 0 ]]; then
            echo "$(date -u +"%FT%TZ") ERROR: Failed to find bucketd instance. " | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
                exit "${RC_NO_BUCKETD_FOUND}"
            else
                echo "$(date -u +"%FT%TZ") INFO: Found bucketd instance ${BUCKETD_HOST}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
            fi
        fi
    fi
}

# shellcheck disable=SC2312
function list_buckets_of_rid() {
    RAFT_OUTPUT=$(curl --silent "http://${BUCKETD_HOST}:${BUCKETD_PORT}/_/raft_sessions/${RAFT_ID}/bucket")
    mapfile -t BUCKETS < <(echo "${RAFT_OUTPUT}" | jq -r '.[] | select((contains("mpuShadowBucket")|not) and (contains("users..bucket")|not))')
    if [[ ${#BUCKETS[@]} -eq 0 ]]; then
        echo "$(date -u +"%FT%TZ") WARNING: No buckets found in raft session ${RAFT_ID}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
        exit "${RC_NO_BUCKETS_FOUND_IN_RID}"
    fi
    echo "$(date -u +"%FT%TZ") INFO: Found ${#BUCKETS[@]} buckets in raft session ${RAFT_ID}" | tee -a "${WORKDIR}/RID_${RAFT_ID}.log"
    UNPROCESSED_BUCKETS=("${BUCKETS[@]}")
}

# shellcheck disable=SC2206
function version_compare() {
    if [[ $1 == "$2" ]]; then
        return 0
    fi
    local IFS=.
    local i
    local ver1=($1)
    local ver2=($2)
    for ((i = ${#ver1[@]}; i < ${#ver2[@]}; i++)); do
        ver1[i]=0
    done
    for ((i = 0; i < ${#ver1[@]}; i++)); do
        if [[ -z ${ver2[i]} ]]; then
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]})); then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]})); then
            return 2
        fi
    done
    return 0
}

function check_min_s3utils_version() {
    version_compare "${S3UTILS_VERSION}" "${S3UTILS_MIN_VERSION}"
    case $? in
    0)
        echo "$(date -u +"%FT%TZ") INFO: The s3utils version ${S3UTILS_VERSION} matches the minimum version allowed." | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        ;;
    1)
        echo "$(date -u +"%FT%TZ") INFO: The s3utils version ${S3UTILS_VERSION} is greater than the minimum version allowed." | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        ;;
    2)
        echo "$(date -u +"%FT%TZ") ERROR: The s3utils version ${S3UTILS_VERSION} does not meet or exceed the minimum version required: ${S3UTILS_MIN_VERSION}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_INVALID_S3UTILS_VERSION}"
        ;;
    *)
        echo "$(date -u +"%FT%TZ") ERROR: Failed to determine s3utils version, version_compare() returned $?" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_AUTO_DETECT_FAILURE}"
        ;;
    esac
}

function check_s3utils_version_exists() {
    for version in "${S3UTILS_VERSIONS[@]}"; do
        if [[ "${version}" == "${S3UTILS_VERSION}" ]]; then
            return 0
        fi
    done
    echo "ERROR: s3utils version ${S3UTILS_VERSION} does not exist on this host." >&2
    exit "${RC_INVALID_S3UTILS_VERSION}"
}

function get_s3utils_versions() {
    mapfile -t S3UTILS_VERSIONS < <(docker images "${S3UTILS_IMAGE}" --format '{{ .Tag }}')
}

function set_s3utils_version() {
    while [[ ${#S3UTILS_VERSIONS[@]} -gt 1 ]]; do
        element1=${S3UTILS_VERSIONS[0]}
        for ((i = 1; i < ${#S3UTILS_VERSIONS[@]}; i++)); do
            element2=${S3UTILS_VERSIONS[i]}
            version_compare "${element1}" "${element2}"
            case $? in
            1)
                S3UTILS_VERSIONS=("${S3UTILS_VERSIONS[@]:0:i}" "${S3UTILS_VERSIONS[@]:${i}+1}")
                i=$((i - 1))
                ;;
            2)
                S3UTILS_VERSIONS=("${S3UTILS_VERSIONS[@]:1:i}" "${S3UTILS_VERSIONS[@]:${i}+1}")
                i=$((i - 1))
                ;;
            esac
        done
    done
    if [[ ${#S3UTILS_VERSIONS[@]} -eq 1 ]]; then
        S3UTILS_VERSION=${S3UTILS_VERSIONS[0]}
        echo "$(date -u +"%FT%TZ") INFO: Detected s3utils version ${S3UTILS_VERSION}." | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        check_min_s3utils_version
    else
        echo "$(date -u +"%FT%TZ") ERROR: Failed to determine s3utils version" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_AUTO_DETECT_FAILURE}"
    fi
}


# shellcheck disable=SC2312
function export_bucket_contents() {
    if ! docker run \
      --rm \
      -it \
      --net host \
      --entrypoint /usr/local/bin/node \
      -e BUCKETD_HOSTPORT="${BUCKETD_HOST}:${BUCKETD_PORT}" \
      -e BUCKETS="${bucket}" \
      -e 'NO_MISSING_KEY_CHECK=1' \
      -e 'VERBOSE=1' \
      "${S3UTILS_IMAGE}:${S3UTILS_VERSION}" \
      verifyBucketSproxydKeys.js \
      >"${WORKDIR}"/raw_"${bucket}"_keys.txt; then
        echo "$(date -u +"%FT%TZ") ERROR: Failed to export keys for ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        echo "$(date -u +"%FT%TZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_EXPORT_S3_KEYS_FAILURE}"
	fi
}

# shellcheck disable=SC2312
function check_for_completed_scan() {
    # See if the raw file contains the message "completed scan"
    SCAN_COMPLETE=$(jq -r '. | select(.message | contains("completed scan"))' "${WORKDIR}/raw_${bucket}_keys.txt" | jq -s length)
    if [[ ${SCAN_COMPLETE} -eq 0 ]]; then
        echo "$(date -u +"%FT%TZ") WARNING: verifyBucketSproxydKeys scan did not complete for ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        exit "${RC_EXPORT_S3_KEYS_FAILURE}"
    else
        echo "$(date -u +"%FT%TZ") INFO: verifyBucketSproxydKeys scan completed for ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
    fi
}

# shellcheck disable=SC2312
function process_bucket_contents() {
    echo "$(date -u +"%FT%TZ") INFO: Processing output of RID ${RAFT_ID} - bucket ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
    if ! jq -r '. | select(.message | contains("sproxyd key"))  + {"bucket": .objectUrl  } | .bucket |= sub("s3://(?<bname>.*)/.*"; .bname) | .objectUrl |= sub("s3://.*/(?<oname>.*)$"; .oname) | [.bucket, .objectUrl, .sproxydKey] | @csv' "${WORKDIR}/raw_${bucket}_keys.txt" >"${WORKDIR}/${bucket}_keys.txt"; then
        echo "$(date -u +"%FT%TZ") ERROR: Failed to process keys for ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        echo "$(date -u +"%FT%TZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_PROCESS_KEYS_FAILURE}"
    fi
}

# shellcheck disable=SC2312
function compare_raw_and_processed_counts() {
    RAW_COUNT=$(jq -r '. | select(.message | contains("sproxyd key"))' "${WORKDIR}/raw_${bucket}_keys.txt" | jq -s length)
    PROCESSED_COUNT=$(wc -l <"${WORKDIR}/${bucket}_keys.txt")
    if [[ ${RAW_COUNT} -ne ${PROCESSED_COUNT} ]]; then
        echo "$(date -u +"%FT%TZ") WARNING: Raw and processed counts do not match for ${bucket} - RAW_COUNT: ${RAW_COUNT} != PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        echo "$(date -u +"%FT%TZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        exit "${RC_COUNT_MISMATCH}"
    else
        echo "$(date -u +"%FT%TZ") INFO: Raw and processed counts match for ${bucket} - RAW_COUNT: ${RAW_COUNT} == PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        # if the raw and processed counts match but are both 0 then print a warning that the bucket is empty
        if [[ ${RAW_COUNT} -eq 0 ]]; then
            echo "$(date -u +"%FT%TZ") WARNING: Bucket ${bucket} appears to be empty: RAW_COUNT: ${RAW_COUNT} == PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        fi
    fi

}

# shellcheck disable=SC2312
function cleanup() {
    bucket_name=$1
    echo "$(date -u +"%FT%TZ") INFO: Removing raw files for ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
    rm -f "${WORKDIR}/raw_${bucket_name}_keys.txt"
}

# shellcheck disable=SC2312
function export_buckets() {
    for bucket in "${BUCKETS[@]}"; do
        echo "$(date -u +"%FT%TZ") INFO: Starting export of ${bucket}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log | tee -a "${WORKDIR}/${bucket}".log
        # Export the bucket contents from the Raft Session
        export_bucket_contents
        # Check for scan completed
        check_for_completed_scan
        # Process the raw file into a csv
        process_bucket_contents
        # Check the raw and processed counts match
        compare_raw_and_processed_counts
        # Pop the bucket out of the UNPROCESSED_BUCKETS array
        unset "UNPROCESSED_BUCKETS[0]"
        UNPROCESSED_BUCKETS=("${UNPROCESSED_BUCKETS[@]}")
        if [[ "${KEEP_RAW_FILES}" == "N" ]]; then
            # Cleanup the raw file for the bucket
            cleanup "${bucket}"
        fi
        echo
    done
    # If the length of UNPROCESSED_BUCKETS is greater than 0 then print a warning
    if [[ ${#UNPROCESSED_BUCKETS[@]} -gt 0 ]]; then
        echo "$(date -u +"%FT%TZ") WARNING: There appear to be unprocessed buckets remaining in RAFT ID ${RAFT_ID}: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
    else
        echo "$(date -u +"%FT%TZ") INFO: All buckets in RAFT ID ${RAFT_ID} appear to have been processed as no UNPROCESSED_BUCKETS remain." | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
    fi
}

function main() {
    init
    if [[ -z ${S3UTILS_VERSION} ]]; then
        echo "$(date -u +"%FT%TZ") INFO: No s3utils version provided. Attempting to auto detect." | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
        set_s3utils_version
    else
        check_s3utils_version_exists
        check_min_s3utils_version
        echo "$(date -u +"%FT%TZ") DEBUG: Using s3utils version ${S3UTILS_VERSION}" | tee -a "${WORKDIR}/RID_${RAFT_ID}".log
    fi
    if [[ -z ${BUCKETD_HOST} ]]; then
        find_bucketd_instance
    fi
    list_buckets_of_rid
    export_buckets
}

# EXECUTION

main
