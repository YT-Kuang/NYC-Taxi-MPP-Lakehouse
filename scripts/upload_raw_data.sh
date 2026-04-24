#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

RAW_DIR="${PROJECT_ROOT}/data/raw"
REFERENCE_FILE="${PROJECT_ROOT}/data/reference/taxi_zone_lookup.csv"

if [ ! -d "${RAW_DIR}" ]; then
  echo "Raw data directory is missing: ${RAW_DIR}" >&2
  exit 1
fi

shopt -s nullglob
raw_files=("${RAW_DIR}"/yellow_tripdata_*.parquet)
if [ "${#raw_files[@]}" -eq 0 ]; then
  echo "No raw parquet files found under ${RAW_DIR}" >&2
  exit 1
fi

for file in "${raw_files[@]}"; do
  name="$(basename "${file}")"
  year="$(sed -E 's/^yellow_tripdata_([0-9]{4})-[0-9]{2}\.parquet$/\1/' <<< "${name}")"
  dest="s3://${S3_BUCKET}/${RAW_PREFIX%/}/year${year}/${name}"
  aws s3 cp "${file}" "${dest}" \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    --only-show-errors
  echo "Uploaded ${dest}"
done

if [ -f "${REFERENCE_FILE}" ]; then
  aws s3 cp "${REFERENCE_FILE}" "s3://${S3_BUCKET}/${REFERENCE_PREFIX%/}/taxi_zone_lookup.csv" \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    --only-show-errors
  echo "Uploaded s3://${S3_BUCKET}/${REFERENCE_PREFIX%/}/taxi_zone_lookup.csv"
else
  echo "Reference data file is missing: ${REFERENCE_FILE}" >&2
  exit 1
fi
