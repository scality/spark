#!/bin/bash

# EXIT CODES
RC_NO_RAFT_ID=10
RC_FILE_PERMISSIONS=11
RC_NO_BUCKETD_FOUND=12
RC_NO_BUCKETS_FOUND_IN_RID=20
RC_EXPORT_S3_KEYS_FAILURE=21
RC_PROCESS_KEYS_FAILURE=22
RC_COUNT_MISMATCH=23

# GLOBALS
BUCKETD_HOST=''
BUCKETS=()
UNPROCESSED_BUCKETS=()
UNBOUND_BUCKETD=('[::]:9000' '0.0.0.0:9000')
WORKDIR=/var/tmp/bucketSproxydKeys

# Check if a raft id was passed
if [[ -z ${1} ]]; then
	echo "Usage: ${0} <raft_id>"
	exit "${RC_NO_RAFT_ID}"
else
	RID=${1}
	echo -e "\nWARNING: Don't forget to check for adequate disk space."
	read -p "Do you want to keep the raw files after processing (make sure adequate space exists)? [y/N] " -n 1 -r
	echo
	KEEP_RAW_FILES=${REPLY}
fi

# FUNCTIONS

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
	if ! [[ -d ${WORKDIR} ]]; then
		if ! mkdir -pv "${WORKDIR}"; then
			echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Failed to create ${WORKDIR}"
			exit "${RC_FILE_PERMISSIONS}"
		else
			echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Created ${WORKDIR}" | tee -a "${WORKDIR}/RID_${RID}.log"
		fi
	fi
}

# shellcheck disable=SC2312
function find_bucketd_instance() {
	# Capture both IPv4 and IPv6 bucketd instances
	SS_OUTPUT=$(ss -tlnp | awk '$4 ~ /^(([0-9]{1,3}\.){3}[0-9]{1,3}|\[::\]):9000$/ {print $4}')
	# If the SS_OUTPUT is equal to the UNBOUND_BUCKETD string then set BUCKETD_HOST to 127.0.0.1
	if [[ ${SS_OUTPUT} == "${UNBOUND_BUCKETD[0]}" ]] || [[ ${SS_OUTPUT} == "${UNBOUND_BUCKETD[1]}" ]]; then
		BUCKETD_HOST="127.0.0.1"
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Found bucketd instance matched UNBOUND_BUCKET. Using BUCKETD_HOST: ${BUCKETD_HOST}" | tee -a "${WORKDIR}/RID_${RID}.log"
	else
		BUCKETD_HOST=$(echo "${SS_OUTPUT}" | cut -d: -f1)
		if [[ -z ${BUCKETD_HOST} ]] || [[ $(is_valid_ip "${BUCKETD_HOST}") == 0 ]]; then
			echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Failed to find bucketd instance" | tee -a "${WORKDIR}/RID_${RID}.log"
			exit "${RC_NO_BUCKETD_FOUND}"
		else
			echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Found bucketd instance ${BUCKETD_HOST}" | tee -a "${WORKDIR}/RID_${RID}.log"
		fi
	fi
}

# shellcheck disable=SC2312
function list_buckets_of_rid() {
	RAFT_OUTPUT=$(curl --silent "http://${BUCKETD_HOST}:9000/_/raft_sessions/${RID}/bucket")
	mapfile -t BUCKETS < <(echo "${RAFT_OUTPUT}" | jq -r '.[] | select (. | contains("mpuShadowBucket") | not) | select (. | contains("users..bucket") | not)')
	if [[ ${#BUCKETS[@]} -eq 0 ]]; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") WARNING: No buckets found in raft session ${RID}" | tee -a "${WORKDIR}/RID_${RID}.log"
		exit "${RC_NO_BUCKETS_FOUND_IN_RID}"
	fi
	echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Found ${#BUCKETS[@]} buckets in raft session ${RID}" | tee -a "${WORKDIR}/RID_${RID}.log"
	UNPROCESSED_BUCKETS=("${BUCKETS[@]}")
}

# shellcheck disable=SC2312
function export_bucket_contents() {
	if ! docker run \
		--rm \
		-it \
		--net host \
		--entrypoint /usr/local/bin/node \
		-e 'BUCKETD_HOSTPORT=127.0.0.1:9000' \
		-e "BUCKETS=${bucket}" \
		-e 'NO_MISSING_KEY_CHECK=1' \
		-e 'VERBOSE=1' \
		registry.scality.com/s3utils/s3utils:1.14.0 \
		verifyBucketSproxydKeys.js \
		>"${WORKDIR}"/raw_"${bucket}"_keys.txt; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Failed to export keys for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RID}".log
		exit "${RC_EXPORT_S3_KEYS_FAILURE}"
	fi
}

# shellcheck disable=SC2312
function check_for_completed_scan() {
	# See if the raw file contains the message "completed scan"
	SCAN_COMPLETE=$(jq -r '. | select(.message | contains("completed scan"))' "${WORKDIR}/raw_${bucket}_keys.txt" | jq -s length)
	if [[ ${SCAN_COMPLETE} -eq 0 ]]; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") WARNING: verifyBucketSproxydKeys scan did not complete for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		exit "${RC_EXPORT_S3_KEYS_FAILURE}"
	else
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: verifyBucketSproxydKeys scan completed for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
	fi
}

# shellcheck disable=SC2312
function process_bucket_contents() {
	echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Processing output of RID ${RID} - bucket ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
	if ! jq -r '. | select(.message | contains("sproxyd key"))  + {"bucket": .objectUrl  } | .bucket |= sub("s3://(?<bname>.*)/.*"; .bname) | .objectUrl |= sub("s3://.*/(?<oname>.*)$"; .oname) | [.bucket, .objectUrl, .sproxydKey] | @csv' "${WORKDIR}/raw_${bucket}_keys.txt" >"${WORKDIR}/${bucket}_keys.txt"; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Failed to process keys for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RID}".log
		exit "${RC_PROCESS_KEYS_FAILURE}"
	fi
}

# shellcheck disable=SC2312
function compare_raw_and_processed_counts() {
	RAW_COUNT=$(jq -r '. | select(.message | contains("sproxyd key"))' "${WORKDIR}/raw_${bucket}_keys.txt" | jq -s length)
	PROCESSED_COUNT=$(wc -l <"${WORKDIR}/${bucket}_keys.txt")
	if [[ ${RAW_COUNT} -ne ${PROCESSED_COUNT} ]]; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") WARNING: Raw and processed counts do not match for ${bucket} - RAW_COUNT: ${RAW_COUNT} != PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") ERROR: Unprocessed buckets: ${UNPROCESSED_BUCKETS[*]}" | tee -a "${WORKDIR}/RID_${RID}".log
		exit "${RC_COUNT_MISMATCH}"
	else
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Raw and processed counts match for ${bucket} - RAW_COUNT: ${RAW_COUNT} == PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		# if the raw and processed counts match but are both 0 then print a warning that the bucket is empty
		if [[ ${RAW_COUNT} -eq 0 ]]; then
			echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") WARNING: Bucket ${bucket} appears to be empty: RAW_COUNT: ${RAW_COUNT} == PROCESED_COUNT: ${PROCESSED_COUNT}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		fi
	fi

}

# shellcheck disable=SC2312
function cleanup() {
	if [[ ${KEEP_RAW_FILES} =~ ^[Yy]$ ]]; then
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Keeping raw files for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
	else
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Removing raw files for ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		rm -f "${WORKDIR}/raw_${bucket}_keys.txt"
	fi
}

# shellcheck disable=SC2312
function export_buckets() {
	for bucket in "${BUCKETS[@]}"; do
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Starting export of ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		# Export the bucket contents from the Raft Session
		export_bucket_contents
		# Check for scan completed
		check_for_completed_scan
		# Process the raw file into a csv
		process_bucket_contents
		# Check the raw and processed counts match
		compare_raw_and_processed_counts
		echo
		echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") INFO: Completed export of ${bucket}" | tee -a "${WORKDIR}/RID_${RID}".log | tee -a "${WORKDIR}/${bucket}".log
		# Pop the bucket out of the UNPROCESSED_BUCKETS array
		UNPROCESSED_BUCKETS=("${UNPROCESSED_BUCKETS[@]/${bucket}/}")
	done
}

function main() {
	init
	find_bucketd_instance
	list_buckets_of_rid
	export_buckets
	cleanup
}

# EXECUTION

main
