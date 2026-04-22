#!/usr/bin/env bash
set -euo pipefail

BUCKET="nyc-taxi-mpp-lakehouse"
REGION="us-east-1"

if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Bucket exists: $BUCKET"
else
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket \
      --bucket "$BUCKET" \
      --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
fi

for key in \
  "raw/" \
  "raw/nyc_taxi/" \
  "raw/nyc_taxi/yellow_tripdata/" \
  "raw/nyc_taxi/yellow_tripdata/year2025/" \
  "raw/nyc_taxi/reference/" \
  "warehouse/" \
  "warehouse/bronze/" \
  "warehouse/silver/" \
  "warehouse/gold/" \
  "tmp/"
do
  aws s3api put-object --bucket "$BUCKET" --key "$key" >/dev/null
  echo "Created s3://$BUCKET/$key"
done