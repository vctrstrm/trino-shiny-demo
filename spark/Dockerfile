ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-alpine3.22

ARG PYTHON_VERSION
ARG SPARK_VERSION=3.5.7
ARG JAVA_VERSION=21

# Install dependencies: wget for downloading, tini as an init process, netcat-openbsd for healthcheck
RUN apk update && \
    apk add --no-cache \
    wget \
    tini \
    curl \
    wget \
    bash \
    procps \
    grep \
    coreutils \
    util-linux \
    openssl \
    ca-certificates \
    tar \
    gzip \
    zip \
    unzip \
    xz \
    gcc \
    make \
    musl-dev \
    linux-headers \
    g++ \
    netcat-openbsd

#################
### SPARK INSTALL
#################

# INSTALL JAVA (gotta be special alpine version because the normal one depends on glibc)
RUN apk add --no-cache \
    && curl -L -o /etc/apk/keys/amazoncorretto.rsa.pub https://apk.corretto.aws/amazoncorretto.rsa.pub \
    && echo "https://apk.corretto.aws" >> /etc/apk/repositories \
    && apk update \
    && apk add amazon-corretto-${JAVA_VERSION}

ENV JAVA_HOME=/usr/lib/jvm/default-jvm

ARG SCALA_VERSION=2.13

# INSTALL SPARK
ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
# Spark distribution name (Scala 2.13 is default for Hadoop 3 build of Spark 3.2+)
ENV SPARK_DIST_NAME="spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}"
ENV SPARK_TGZ_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_DIST_NAME}.tgz"

# Download and install Spark
RUN wget --progress=bar:force:noscroll -O /tmp/spark.tgz "$SPARK_TGZ_URL" && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    rm /tmp/spark.tgz && \
    mv "/opt/${SPARK_DIST_NAME}" "${SPARK_HOME}"

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark[sql,pandas_on_spark,connect]==${SPARK_VERSION} \
    pandas \
    setuptools

##############
### EXTRA JARS
##############

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-connect_${SCALA_VERSION}/${SPARK_VERSION}/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    -o $SPARK_HOME/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    && chown root:root $SPARK_HOME/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar \
    && chmod 644 $SPARK_HOME/jars/spark-connect_${SCALA_VERSION}-${SPARK_VERSION}.jar

ARG NESSIE_VERSION=0.105.7
ARG AWS_SDK_VERSION=2.40.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.10.0
RUN _custom_jars_dir="${SPARK_HOME}/custom-jars/" \
    && mkdir -p "${_custom_jars_dir}" \
    && wget -P "${_custom_jars_dir}/" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_SPARK_RUNTIME_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_${SCALA_VERSION}/${NESSIE_VERSION}/nessie-spark-extensions-3.5_${SCALA_VERSION}-${NESSIE_VERSION}.jar" \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_VERSION}/url-connection-client-${AWS_SDK_VERSION}.jar" \
    && chown -R root:root "$_custom_jars_dir" \
    && chmod -R +rX "$_custom_jars_dir"

ARG VAST_SPARK_PLUGIN_ZIP_URL=https://github.com/vast-data/vast-db-connectors/releases/download/spark3-vast-3.5.1-e330d19e3523/spark3-vast-3.5.1-e330d19e3523.zip
RUN \
    echo "[BUILD] Downloading/Unpacking VAST DB Spark Connector JAR's" \
    && __vast_trino_tmp_dir="/tmp/vast" \
    && __vast_trino_tmp_zip_file="${__vast_trino_tmp_dir}/vast-trino-jars.zip" \
    && __vast_trino_plugin_dir="${SPARK_HOME}/vast-jars" \
    && mkdir -p "${__vast_trino_tmp_dir}" "${__vast_trino_plugin_dir}" \
    && wget -O "${__vast_trino_tmp_zip_file}" "${VAST_SPARK_PLUGIN_ZIP_URL}" \
    && unzip "${__vast_trino_tmp_zip_file}" -d "${__vast_trino_tmp_dir}" \
    && find "${__vast_trino_tmp_dir}" -type f -name "*.jar" -exec cp '{}' "${__vast_trino_plugin_dir}/" ';' \
    && rm -rf "${__vast_trino_tmp_dir}"

ARG POSTGRES_JDBC_VERSION=42.7.8
RUN \
    echo "[BUILD] Downloading PostgreSQL JDBC Driver" \
    && __postgresql_jdbc_dir="${SPARK_HOME}/custom-jars" \
    && mkdir -p "${__postgresql_jdbc_dir}" \
    && wget -O "${__postgresql_jdbc_dir}/postgresql-${POSTGRES_JDBC_VERSION}.jar" "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && chown root:root "${__postgresql_jdbc_dir}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && chmod 644 "${__postgresql_jdbc_dir}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

ARG TRINO_JDBC_VERSION=475
RUN \
    echo "[BUILD] Downloading Trino JDBC Driver" \
    && __trino_jdbc_dir="${SPARK_HOME}/custom-jars" \
    && mkdir -p "${__trino_jdbc_dir}" \
    && wget -O "${__trino_jdbc_dir}/trino-jdbc-${TRINO_JDBC_VERSION}.jar" "https://repo1.maven.org/maven2/io/trino/trino-jdbc/${TRINO_JDBC_VERSION}/trino-jdbc-${TRINO_JDBC_VERSION}.jar" \
    && chown root:root "${__trino_jdbc_dir}/trino-jdbc-${TRINO_JDBC_VERSION}.jar" \
    && chmod 644 "${__trino_jdbc_dir}/trino-jdbc-${TRINO_JDBC_VERSION}.jar"

######################
### ENTRYPOINT CONFIG
######################

# Copy misc scripts
COPY --chown=root:root --chmod=755 ./misc-scripts $SPARK_HOME/misc-scripts

# Copy custom configuration files
COPY --chown=root:root --chmod=644 conf/spark-defaults.conf $SPARK_CONF_DIR/spark-defaults.conf
COPY --chown=root:root --chmod=644 conf/spark-env.sh $SPARK_CONF_DIR/spark-env.sh
RUN chmod +x $SPARK_CONF_DIR/spark-env.sh

# Copy entrypoint script
COPY --chown=root:root --chmod=755 entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Create directory for Spark event logs and make it world-writable
RUN mkdir -p /tmp/spark-events && chmod -R 777 /tmp/spark-events
# Create directories for Spark logs and run files (PIDs)
RUN mkdir -p $SPARK_HOME/logs && chmod -R 777 $SPARK_HOME/logs
RUN mkdir -p $SPARK_HOME/run && chmod -R 777 $SPARK_HOME/run

COPY --chown=root:root --chmod=755 healthcheck.sh /usr/local/bin/healthcheck.sh
RUN chmod +x /usr/local/bin/healthcheck.sh

# Default Spark Ports - can be overridden at build time or runtime
ENV \
 SPARK_MASTER_PORT=7077 \
 SPARK_MASTER_WEBUI_PORT=8080 \
 SPARK_WORKER_WEBUI_PORT=6066 \
 SPARK_THRIFT_PORT=10000 \
 SPARK_CONNECT_PORT=15002 \
 SPARK_UI_PORT=4040

# Expose ports using environment variables

# Spark Master UI and RPC
EXPOSE ${SPARK_MASTER_WEBUI_PORT}
EXPOSE ${SPARK_MASTER_PORT}
# Spark Worker UI and RPC (if SPARK_WORKER_PORT is set)
EXPOSE ${SPARK_WORKER_WEBUI_PORT}
# EXPOSE ${SPARK_WORKER_PORT} # Worker RPC port is often random unless explicitly set
# Thrift JDBC/ODBC Server
EXPOSE ${SPARK_THRIFT_PORT}
# Spark Connect Server
EXPOSE ${SPARK_CONNECT_PORT}
# Spark application UI (standard port)
EXPOSE ${SPARK_UI_PORT}

WORKDIR $SPARK_HOME

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=5 \
  CMD /usr/local/bin/healthcheck.sh

ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/entrypoint.sh"] 
