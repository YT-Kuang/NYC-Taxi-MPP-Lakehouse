#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../scripts/env.sh"

if aws s3api head-bucket --bucket "$S3_BUCKET" --profile "$AWS_PROFILE" --region "$AWS_REGION" 2>/dev/null; then
  echo "Bucket exists: $S3_BUCKET"
else
  if [ "$AWS_REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION" --profile "$AWS_PROFILE"
  else
    aws s3api create-bucket \
      --bucket "$S3_BUCKET" \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE" \
      --create-bucket-configuration LocationConstraint="$AWS_REGION"
  fi
fi

for key in \
  "${RAW_PREFIX%/}/" \
  "${RAW_PREFIX%/}/year2025/" \
  "${REFERENCE_PREFIX%/}/" \
  "${WAREHOUSE_PREFIX%/}/" \
  "tmp/"
do
  aws s3api put-object --bucket "$S3_BUCKET" --key "$key" --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null
  echo "Created s3://$S3_BUCKET/$key"
done
