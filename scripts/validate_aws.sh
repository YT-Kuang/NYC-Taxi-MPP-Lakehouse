#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

command -v aws >/dev/null 2>&1 || {
  echo "aws CLI is required but was not found on PATH." >&2
  exit 1
}

if [ ! -d "${AWS_CONFIG_DIR}" ]; then
  echo "AWS_CONFIG_DIR does not exist: ${AWS_CONFIG_DIR}" >&2
  exit 1
fi

aws sts get-caller-identity --profile "${AWS_PROFILE}" --region "${AWS_REGION}" >/dev/null
echo "AWS credentials are valid for profile ${AWS_PROFILE} in ${AWS_REGION}."
