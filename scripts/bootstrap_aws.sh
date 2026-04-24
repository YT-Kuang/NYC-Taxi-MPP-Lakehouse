#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

"${SCRIPT_DIR}/validate_aws.sh"
"${PROJECT_ROOT}/infra/setup_bucket.sh"

for database in "${GLUE_DATABASE_RAW}" "${GLUE_DATABASE_STAGING}" "${GLUE_DATABASE_GOLD}"; do
  if aws glue get-database \
    --profile "${AWS_PROFILE}" \
    --region "${AWS_REGION}" \
    --name "${database}" >/dev/null 2>&1; then
    echo "Glue database exists: ${database}"
  else
    aws glue create-database \
      --profile "${AWS_PROFILE}" \
      --region "${AWS_REGION}" \
      --database-input "Name=${database}" >/dev/null
    echo "Created Glue database: ${database}"
  fi
done
