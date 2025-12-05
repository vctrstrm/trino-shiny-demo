#!/usr/bin/env bash
set -Eeufo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIST_JARS_SCRIPT="${SCRIPT_DIR}/list-jars.py"

function test_list_jars() (
  set -Eeufo pipefail

  echo "[INFO] Testing list jars..."

  SPARK_PRINT_LAUNCH_COMMAND=1 \
  spark-submit \
    --master local[1] \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.host=localhost \
    "${LIST_JARS_SCRIPT}"

  echo "[INFO] List jars test completed successfully."
)

function test_run_pi() (
  set -Eeufo pipefail

  echo "[INFO] Testing Spark local mode..."

  SPARK_PRINT_LAUNCH_COMMAND=1 \
    spark-submit \
      -v \
      --master local[1] \
      --conf spark.driver.bindAddress=0.0.0.0 \
      --conf spark.driver.host=localhost \
      --class org.apache.spark.examples.SparkPi \
      examples/jars/spark-examples_2.13-3.5.6.jar 10

  echo "[INFO] Spark local mode test completed successfully."
)

function cli() (
  set -Eeufo pipefail
  local command="${1:-}"
  
  case "$command" in
    "example")
      test_run_pi
      ;;
    "list-jars")
      test_list_jars
      ;;
    *)
      echo "Usage: $0 {example|list-jars}"
      echo "  example    - Run Spark Pi example"
      echo "  list-jars  - Test list jars functionality"
      exit 1
      ;;
  esac
)

function main() (
  set -Eeufo pipefail

  cli "$@"
)

main "$@"