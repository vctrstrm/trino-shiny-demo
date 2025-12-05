#!/usr/bin/env bash

# Set SPARK_MASTER_HOST to 0.0.0.0 to allow binding to all network interfaces,
# making the Master UI and RPC accessible from outside the container (via mapped ports).
export SPARK_MASTER_HOST="0.0.0.0"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"
export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"

# Set SPARK_WORKER_WEBUI_PORT. Worker host binding will be set via --host in entrypoint.sh.
export SPARK_WORKER_WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"

# Optional: Define worker resources
export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"

# Specify directories for Spark daemon logs and PID files
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-${SPARK_HOME}/logs}"
export SPARK_PID_DIR="${SPARK_PID_DIR:-${SPARK_HOME}/run}"

# Example: Java options for Spark daemons
# export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=NONE"

# If JAVA_HOME is not correctly picked up from the base image, uncomment and set it.
# export JAVA_HOME=/opt/java/openjdk # Example path, verify with base image 

if [[ -n "${VAST_ACCESS_KEY:-}" ]]; then
  export AWS_ACCESS_KEY_ID="${VAST_ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${VAST_SECRET_ACCESS_KEY}"
  export AWS_REGION="us-east-1"
  export AWS_ENDPOINT="${VAST_ENDPOINT}"
  export AWS_PATH_STYLE_ACCESS="true"
fi