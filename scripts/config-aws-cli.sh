#!/usr/bin/env bash
set -Eeufo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.conf
source "${SCRIPT_DIR}/common.conf"

AWS_PROFILE="${AWS_PROFILE:-vast}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-${HOME}/.aws/config}"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-${HOME}/.aws/credentials}"

function main() (
	echo "[INFO] Configuring AWS CLI with VAST credentials for profile ${AWS_PROFILE}"

	aws configure set request_checksum_calculation when_required --profile "${AWS_PROFILE}"
	aws configure set endpoint_url "${VAST_ENDPOINT}" --profile "${AWS_PROFILE}"
	aws configure set region "us-east-1" --profile "${AWS_PROFILE}"
	aws configure set aws_access_key_id "${VAST_ACCESS_KEY}" --profile "${AWS_PROFILE}"
	aws configure set aws_secret_access_key "${VAST_SECRET_ACCESS_KEY}" --profile "${AWS_PROFILE}"

	echo "[INFO] Done."
)

main "$@"
