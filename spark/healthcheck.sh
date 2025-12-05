#!/usr/bin/env bash
set -Eeuo pipefail; shopt -s failglob;

# Use environment variables for ports, falling back to defaults if not set
# These defaults should match the ENV defaults in the Dockerfile
MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT:-8080}
WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}
THRIFT_PORT=${SPARK_THRIFT_PORT:-10000}
CONNECT_PORT=${SPARK_CONNECT_PORT:-15002}

# Check Spark Master UI
if ! curl -sf "http://127.0.0.1:${MASTER_WEBUI_PORT}" > /dev/null; then
  echo "Spark Master UI on port ${MASTER_WEBUI_PORT} is not responding."
  exit 1
fi
echo "Spark Master UI OK."

# Check Spark Worker UI
# Note: Worker might take a bit longer to register and start its UI fully.
# A simple port check is a good first step.
if ! curl -sf "http://127.0.0.1:${WORKER_WEBUI_PORT}" > /dev/null; then
  echo "Spark Worker UI on port ${WORKER_WEBUI_PORT} is not responding."
  exit 1
fi
echo "Spark Worker UI OK."

# Check Thrift Server using netcat (nc)
# Ensure 'nc' is available in the container (added netcat-openbsd in Dockerfile)
if ! nc -z 127.0.0.1 "${THRIFT_PORT}"; then
  echo "Thrift server on port ${THRIFT_PORT} is not responding."
  exit 1
fi
echo "Thrift Server OK."

# Check Spark Connect Server using netcat (nc)
if ! nc -z 127.0.0.1 "${CONNECT_PORT}"; then
  echo "Spark Connect server on port ${CONNECT_PORT} is not responding."
  exit 1
fi
echo "Spark Connect Server OK."

echo "All Spark services appear to be healthy."
exit 0