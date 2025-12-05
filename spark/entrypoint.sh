#!/bin/bash
set -Eeuo pipefail; shopt -s failglob

SPARK_CUSTOM_JARS_DIR="$SPARK_HOME/custom-jars"
SPARK_VAST_JARS_DIR="$SPARK_HOME/vast-jars"
SPARK_DEFAULTS_CONFIG="$SPARK_CONF_DIR/spark-defaults.conf"

: "${SPARK_USE_CUSTOM_JARS:=yes}"
: "${SPARK_USE_VAST_JARS:=yes}"


# Build classpath configuration based on enabled jar directories
EXTRA_CLASSPATH=""

if [[ "$SPARK_USE_CUSTOM_JARS" == "yes" ]] && [[ -d "$SPARK_CUSTOM_JARS_DIR" ]]; then
    EXTRA_CLASSPATH="$SPARK_CUSTOM_JARS_DIR/*"
fi

if [[ "$SPARK_USE_VAST_JARS" == "yes" ]] && [[ -d "$SPARK_VAST_JARS_DIR" ]]; then
    if [[ -n "$EXTRA_CLASSPATH" ]]; then
        EXTRA_CLASSPATH="$EXTRA_CLASSPATH:$SPARK_VAST_JARS_DIR/*"
    else
        EXTRA_CLASSPATH="$SPARK_VAST_JARS_DIR/*"
    fi
fi

# Update spark-defaults.conf with the classpath configuration
if [[ -n "$EXTRA_CLASSPATH" ]]; then
    # Remove any existing spark.driver.extraClassPath and spark.executor.extraClassPath lines
    sed -i '/^spark\.driver\.extraClassPath\s/d' "$SPARK_DEFAULTS_CONFIG"
    sed -i '/^spark\.executor\.extraClassPath\s/d' "$SPARK_DEFAULTS_CONFIG"
    # Add the new classpath configuration
    printf "\n%s\n" "spark.driver.extraClassPath   $EXTRA_CLASSPATH" >> "$SPARK_DEFAULTS_CONFIG"
    printf "%s\n" "spark.executor.extraClassPath $EXTRA_CLASSPATH" >> "$SPARK_DEFAULTS_CONFIG"
    echo "Added extraClassPath configuration with custom and vast jars."
fi

: "${SPARK_DRIVER_PRIORITIZE_USER_JARS:=yes}"
: "${SPARK_EXECUTOR_PRIORITIZE_USER_JARS:=yes}"

# Add user class path prioritization configuration if enabled
if [[ "$SPARK_DRIVER_PRIORITIZE_USER_JARS" == "yes" ]]; then
    # Remove any existing spark.driver.userClassPathFirst line
    sed -i '/^spark\.driver\.userClassPathFirst\s/d' "$SPARK_DEFAULTS_CONFIG"
    # Add the configuration
    printf "\n%s\n" "spark.driver.userClassPathFirst true" >> "$SPARK_DEFAULTS_CONFIG"
    echo "Added spark.driver.userClassPathFirst=true configuration."
fi

if [[ "$SPARK_EXECUTOR_PRIORITIZE_USER_JARS" == "yes" ]]; then
    # Remove any existing spark.executor.userClassPathFirst line
    sed -i '/^spark\.executor\.userClassPathFirst\s/d' "$SPARK_DEFAULTS_CONFIG"
    # Add the configuration
    printf "\n%s\n" "spark.executor.userClassPathFirst true" >> "$SPARK_DEFAULTS_CONFIG"
    echo "Added spark.executor.userClassPathFirst=true configuration."
fi


: "${SPARK_CONFIGURE_DEFAULT_EXTENSIONS:=yes}"

if [[ "$SPARK_CONFIGURE_DEFAULT_EXTENSIONS" == "yes" ]]; then
  # Configure extensions
  echo "[INFO] Configuring Spark extensions..."
  cat <<EOFXXX >> "$SPARK_DEFAULTS_CONFIG"

# EXTENSION CONFIG
spark.sql.extensions=ndb.NDBSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions
spark.sql.readSideCharPadding=False

spark.sql.catalogImplementation=in-memory
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.spark_catalog.uri=$ICEBERG_NESSIE_URI
spark.sql.catalog.spark_catalog.ref=main
spark.sql.catalog.spark_catalog.authentication.type=NONE
spark.sql.catalog.spark_catalog.warehouse=$ICEBERG_NESSIE_DEFAULT_WAREHOUSE_DIR
spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.spark_catalog.s3.endpoint=$VAST_ENDPOINT
spark.sql.catalog.spark_catalog.client.region=us-east-1
spark.sql.catalog.spark_catalog.s3.path-style-access=true

spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.nessie.uri=$ICEBERG_NESSIE_URI
spark.sql.catalog.nessie.ref=main
spark.sql.catalog.nessie.authentication.type=NONE
spark.sql.catalog.nessie.warehouse=$ICEBERG_NESSIE_DEFAULT_WAREHOUSE_DIR
spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.nessie.s3.endpoint=$VAST_ENDPOINT
spark.sql.catalog.nessie.client.region=us-east-1
spark.sql.catalog.nessie.s3.path-style-access=true

spark.sql.catalog.ndb=spark.sql.catalog.ndb.VastCatalog
spark.ndb.access_key_id=$VAST_ACCESS_KEY
spark.ndb.secret_access_key=$VAST_SECRET_ACCESS_KEY
spark.ndb.endpoint=$VAST_ENDPOINT
spark.ndb.data_endpoints=$VAST_DATA_ENDPOINTS

EOFXXX
  echo "[INFO] Spark extensions configured."
fi

##########################
## START SPARK SERVICES ##
##########################

echo "Starting Spark services..."

# Ensure Spark environment variables are exported for child processes
export SPARK_MASTER_PORT SPARK_MASTER_WEBUI_PORT SPARK_WORKER_WEBUI_PORT SPARK_THRIFT_PORT SPARK_CONNECT_PORT SPARK_UI_PORT

# Start Spark Master
# Ports are typically configured via spark-env.sh or spark-defaults.conf,
# referencing the ENV vars set in the Dockerfile.
# The --host 0.0.0.0 binding is usually handled by SPARK_MASTER_HOST in spark-env.sh or defaults
"$SPARK_HOME/sbin/start-master.sh"
echo "Spark Master started. Web UI should be at http://<host_ip>:${SPARK_MASTER_WEBUI_PORT}"

# Internal Master URL for other services within the container/cluster
# Use 127.0.0.1 for local resolution, the port comes from the ENV var
MASTER_URL="spark://127.0.0.1:${SPARK_MASTER_PORT}"

# Start Spark Worker
# The worker registers with the master at $MASTER_URL.
# --host 0.0.0.0 binding is handled by SPARK_WORKER_HOST or defaults
# --webui-port uses the ENV var
"$SPARK_HOME/sbin/start-worker.sh" \
  "$MASTER_URL" \
  --host 0.0.0.0 \
  --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
  --cores "${SPARK_WORKER_CORES:-4}" \
  --memory "${SPARK_WORKER_MEMORY:-4g}"
  
echo "Spark Worker started and registering with Master. Web UI should be at http://<host_ip>:${SPARK_WORKER_WEBUI_PORT}"

# Start Thrift JDBC/ODBC server
# Pass the port via hiveconf, referencing the ENV var
"$SPARK_HOME/sbin/start-thriftserver.sh" \
  --master "$MASTER_URL" \
  --hiveconf hive.server2.thrift.port="${SPARK_THRIFT_PORT}" \
  --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
  --executor-memory ${SPARK_THRIFT_EXECUTOR_MEMORY:-1g} \
  --executor-cores ${SPARK_THRIFT_EXECUTOR_CORES:-1} \
  --total-executor-cores ${SPARK_THRIFT_TOTAL_EXECUTOR_CORES:-1} \
  --conf spark.executor.instances=${SPARK_THRIFT_EXECUTOR_INSTANCES:-1}
echo "Spark Thrift JDBC/ODBC server started on port ${SPARK_THRIFT_PORT}."

# depends on the image, hence hardcoded
SPARK_VERSION=3.5.6
# Start Spark Connect server
# Pass the port via spark conf, referencing the ENV var
## to fetch libs online, use --packages
## --packages "org.apache.spark:spark-connect_2.13:${SPARK_VERSION}"
"$SPARK_HOME/sbin/start-connect-server.sh" \
  --master "$MASTER_URL" \
  --conf spark.connect.grpc.binding.address=0.0.0.0 \
  --conf spark.connect.grpc.binding.port="${SPARK_CONNECT_PORT}" \
  --executor-memory ${SPARK_CONNECT_EXECUTOR_MEMORY:-1g} \
  --executor-cores ${SPARK_CONNECT_EXECUTOR_CORES:-1} \
  --total-executor-cores ${SPARK_CONNECT_TOTAL_EXECUTOR_CORES:-1}
echo "Spark Connect server started on port ${SPARK_CONNECT_PORT}."

echo ""
echo "All Spark services are starting."
echo "To access services from your host machine, ensure ports are mapped (e.g., docker run -p ${SPARK_MASTER_WEBUI_PORT}:${SPARK_MASTER_WEBUI_PORT} ...)."
echo "  Spark Master UI: http://localhost:${SPARK_MASTER_WEBUI_PORT}"
echo "  Spark Worker UI: http://localhost:${SPARK_WORKER_WEBUI_PORT}"
echo "  Thrift Server JDBC: jdbc:hive2://localhost:${SPARK_THRIFT_PORT}"
echo "  Spark Connect: sc://localhost:${SPARK_CONNECT_PORT}"
echo ""
echo "Tailing Spark service logs..."

tail -F /opt/spark/logs/spark--org.apache.spark.deploy.master.Master-1-spark.out | sed 's/^/[spark-master] /' &
tail -F /opt/spark/logs/spark--org.apache.spark.deploy.worker.Worker-1-spark.out | sed 's/^/[spark-worker] /' &
tail -F /opt/spark/logs/spark--org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1-spark.out | sed 's/^/[spark-thriftserver] /' &
tail -F /opt/spark/logs/spark--org.apache.spark.sql.connect.service.SparkConnectServer-1-spark.out | sed 's/^/[spark-connect] /' &

echo ""
echo "Tailing /dev/null to keep container running. Use Ctrl+C or 'docker stop <container_id>' to stop."
# Keep the container alive. Tini (as PID 1) will handle signals.
tail -f /dev/null 