#!/usr/bin/env bash
set -Eeufo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.conf
source "${SCRIPT_DIR}/common.conf"

OS_ARCH="${OS_ARCH:-amd64}"
OS_NAME="${OS_NAME:-linux}"

function detect_platform() (
	set -o nounset -o errexit -o pipefail

	local os_name arch_name

	# --- OS detection (with overrides) ---
	if [[ -n "${OS_NAME:-}" ]]; then
		case "${OS_NAME}" in
		linux | Linux)
			os_name="linux"
			;;
		darwin | Darwin | macos | MacOS | macOS | OSX | osx)
			os_name="darwin"
			;;
		*)
			echo "[ERROR] Unsupported OS_NAME override: ${OS_NAME}" >&2
			exit 1
			;;
		esac
	else
		case "$(uname -s)" in
		Linux)
			os_name="linux"
			;;
		Darwin)
			os_name="darwin"
			;;
		*)
			echo "[ERROR] Unsupported operating system: $(uname -s). Set OS_NAME to override." >&2
			exit 1
			;;
		esac
	fi

	# --- ARCH detection (with overrides) ---
	if [[ -n "${OS_ARCH:-}" ]]; then
		case "${OS_ARCH}" in
		amd64 | x86_64)
			arch_name="amd64"
			;;
		arm64 | aarch64)
			arch_name="arm64"
			;;
		*)
			echo "[ERROR] Unsupported OS_ARCH override: ${OS_ARCH}" >&2
			exit 1
			;;
		esac
	else
		case "$(uname -m)" in
		x86_64 | amd64)
			arch_name="amd64"
			;;
		arm64 | aarch64)
			arch_name="arm64"
			;;
		*)
			echo "[ERROR] Unsupported architecture: $(uname -m). Set OS_ARCH to override." >&2
			exit 1
			;;
		esac
	fi

	# --- final combination check ---
	case "${os_name}/${arch_name}" in
	linux/amd64 | linux/arm64 | darwin/amd64 | darwin/arm64) ;;
	*)
		echo "[ERROR] Unsupported OS/arch combination: ${os_name}/${arch_name}" >&2
		exit 1
		;;
	esac

	echo "${os_name} ${arch_name}"
)

function ensure_trino_cli() (
	set -o nounset -o errexit -o pipefail

	local trino_cli_url="https://repo.maven.apache.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar"
	local trino_cli_jar_path="${BIN_ROOT}/trino"

	if ! [[ -f "${trino_cli_jar_path}" ]]; then
		echo "[INFO] Downloading Trino CLI"
		curl -fsSL -o "${trino_cli_jar_path}" "${trino_cli_url}"
		chmod +x "${trino_cli_jar_path}"
	fi

	if ! command -v java >/dev/null 2>&1; then
		echo "[WARNING] 'java' is not installed or not in PATH. The local Trino CLI client outside of Docker may not work." >&2
	fi
)

function ensure_nessie_cli() (
	set -o nounset -o errexit -o pipefail

	local nessie_cli_url="https://github.com/projectnessie/nessie/releases/download/nessie-${NESSIE_VERSION}/nessie-cli-${NESSIE_VERSION}-runner.jar"
	local nessie_cli_jar_path="${BIN_ROOT}/nessie-cli"

	if ! [[ -f "${nessie_cli_jar_path}" ]]; then
		echo "[INFO] Downloading Nessie CLI"
		curl -fsSL -o "${nessie_cli_jar_path}" "${nessie_cli_url}"
		chmod +x "${nessie_cli_jar_path}"
	fi

	if ! command -v java >/dev/null 2>&1; then
		echo "[WARNING] 'java' is not installed or not in PATH. The local Nessie CLI client outside of Docker may not work." >&2
	fi
)

function ensure_bin_dir() (
	set -o nounset -o errexit -o pipefail

	if ! [[ -d "${BIN_ROOT}" ]]; then
		mkdir -p "${BIN_ROOT}"
	fi
)

function init_env_file() (
	set -o nounset -o errexit -o pipefail

	local DEFAULT_ENV_FILE="${REPO_ROOT}/.default.env"
	local ENV_FILE="${REPO_ROOT}/.env"

	if [[ -f "${ENV_FILE}" ]]; then
		echo "[INFO] Environment file already exists (you can edit it to your needs)"
		return 0
	fi

	echo "[INFO] Copying default environment file to environment file"
	cp "${DEFAULT_ENV_FILE}" "${ENV_FILE}"
	echo "[INFO] Make sure to edit the environment file to your needs"
)

function main() (
	set -o nounset -o errexit -o pipefail

	# Early fail if the platform is unsupported. This also validates any overrides.
	detect_platform >/dev/null

	echo "[INFO] Initializing VAST Range"
	cd "${REPO_ROOT}"

	ensure_bin_dir
	ensure_trino_cli
	ensure_nessie_cli
  init_env_file

	echo "[INFO] Done."
)

main "$@"
