#!/usr/bin/env bash
set -Eeufo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.conf
source "${SCRIPT_DIR}/common.conf"

VAST_AWS_PROFILE="${VAST_AWS_PROFILE:-vast}"
LOCAL_AWS_PROFILE="${LOCAL_AWS_PROFILE:-local}"
AWS_CONFIG_FILE="${AWS_CONFIG_FILE:-${HOME}/.aws/config}"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-${HOME}/.aws/credentials}"

function config_vast_credentials() (
	set -o nounset -o errexit -o pipefail

	local aws_profile="${VAST_AWS_PROFILE}"

	echo "[INFO] Configuring AWS CLI with VAST credentials for profile ${aws_profile}"

	aws configure set request_checksum_calculation when_required --profile "${aws_profile}"
	aws configure set endpoint_url "${VAST_ENDPOINT}" --profile "${aws_profile}"
	aws configure set region "us-east-1" --profile "${aws_profile}"
	aws configure set aws_access_key_id "${VAST_ACCESS_KEY}" --profile "${aws_profile}"
	aws configure set aws_secret_access_key "${VAST_SECRET_ACCESS_KEY}" --profile "${aws_profile}"
	aws configure set s3.signature_version s3v4 --profile "${aws_profile}"

	echo "[INFO] Done."
)

function config_local_credentials() (
	set -o nounset -o errexit -o pipefail

	local aws_profile="${LOCAL_AWS_PROFILE}"

	echo "[INFO] Configuring AWS CLI with LOCAL credentials for profile ${aws_profile}"

	aws configure set request_checksum_calculation when_required --profile "${aws_profile}"
	aws configure set endpoint_url "${LOCAL_S3_DOCKER_EXTERNAL_ENDPOINT}" --profile "${aws_profile}"
	aws configure set region "us-east-1" --profile "${aws_profile}"
	aws configure set aws_access_key_id "${LOCAL_S3_ACCESS_KEY}" --profile "${aws_profile}"
	aws configure set aws_secret_access_key "${LOCAL_S3_SECRET_ACCESS_KEY}" --profile "${aws_profile}"
	aws configure set s3.signature_version s3v4 --profile "${aws_profile}"

	echo "[INFO] Done."
)

function main() (
	set -o nounset -o errexit -o pipefail

	config_vast_credentials
	config_local_credentials
)

main "$@"
