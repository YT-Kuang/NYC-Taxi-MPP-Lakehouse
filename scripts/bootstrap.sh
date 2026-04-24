#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

"${SCRIPT_DIR}/setup_local_python.sh"
"${SCRIPT_DIR}/bootstrap_aws.sh"
"${SCRIPT_DIR}/upload_raw_data.sh"
docker compose up -d --build
"${SCRIPT_DIR}/validate_stack.sh"

echo "Bootstrap complete."
