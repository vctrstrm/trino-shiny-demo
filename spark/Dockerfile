# =========================================================
# Py Builder Image
# =========================================================

FROM ghcr.io/astral-sh/uv:bookworm-slim AS py_builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
ARG PYTHON_VERSION=3.11

ENV \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_CACHE_DIR=/var/cache/uv \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_COMPILE_BYTECODE=1

# Install CPython via uv and create a shared venv at a fixed path
RUN --mount=type=cache,target=/var/cache/uv \
    uv python install ${PYTHON_VERSION} && \
    uv venv -p ${PYTHON_VERSION} /opt/venv && \
    uv cache prune --ci

# Activate shared venv for everything that follows
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

ARG SPARK_VERSION=3.5.7
RUN --mount=type=cache,target=/var/cache/uv \
    uv pip install \
        pandas \
        numpy \
        pyarrow \
        plotly \
        matplotlib \
        seaborn \
        scikit-learn \
        trino \
        boto3 \
        pyspark[sql,pandas_on_spark,connect]==${SPARK_VERSION}

# =========================================================
# Final Image
# =========================================================

FROM debian:bookworm-slim

ARG BUILD_OS=linux
ARG BUILD_ARCH=amd64

RUN apt-get update && apt-get install --yes \
    git \
    git-lfs \
    vim \
    bash \
    bash-completion \
    ca-certificates \
    curl \
    dnsutils \
    findutils \
    ipmitool \
    iproute2 \
    iputils-ping \
    tree \
    rsync \
    wget \
    zip \
    unzip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

ARG UV_VERSION=0.9.15
RUN --mount=type=tmpfs,target=/tmp \
    wget -O /tmp/install_uv.sh \
        https://astral.sh/uv/${UV_VERSION}/install.sh \
    && UV_INSTALL_DIR=/opt/uv UV_NO_MODIFY_PATH=1 sh /tmp/install_uv.sh \
    && chown -R root:root /opt/uv \
    && ln -s /opt/uv/uv /usr/local/bin/uv \
    && ln -s /opt/uv/uvx /usr/local/bin/uvx \
    && rm -f /tmp/install_uv.sh

# check for versions here: https://raw.githubusercontent.com/aws/aws-cli/v2/CHANGELOG.rst
ARG AWS_CLI_VERSION=2.32.10
# todo: add support for other os/arch combos
RUN --mount=type=tmpfs,target=/tmp \
    wget -O /tmp/awscliv2.zip \
        https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWS_CLI_VERSION}.zip \
    && unzip /tmp/awscliv2.zip -d /tmp/ \
    && /tmp/aws/install \
    && rm -rf /tmp/aws /tmp/awscliv2.zip

ARG JAVA_VERSION=21
ENV JAVA_VERSION=${JAVA_VERSION}
ENV JAVA_HOME=/opt/java/amazon-corretto-${JAVA_VERSION}
RUN mkdir -p /opt/java \
    && curl -L https://corretto.aws/downloads/latest/amazon-corretto-${JAVA_VERSION}-x64-linux-jdk.tar.gz -o /tmp/jdk.tgz \
    && tar -xzf /tmp/jdk.tgz -C /opt/java \
    && ln -s /opt/java/amazon-corretto-* ${JAVA_HOME} \
    && rm /tmp/jdk.tgz

ENV PATH="${JAVA_HOME}/bin:$PATH"

ARG SPARK_VERSION=3.5.7
ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
# Spark distribution name (Scala 2.13 is default for Hadoop 3 build of Spark 3.2+)
ARG SPARK_DIST_NAME="spark-${SPARK_VERSION}-bin-hadoop3-scala2.13"
ARG SPARK_TGZ_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_DIST_NAME}.tgz"

RUN curl -L "$SPARK_TGZ_URL" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    rm /tmp/spark.tgz && \
    mv "/opt/${SPARK_DIST_NAME}" "${SPARK_HOME}" \
    && chown -R root:root "${SPARK_HOME}" \
    && chmod -R +rX "${SPARK_HOME}"

ENV PATH="$SPARK_HOME/bin:$PATH"

##############
### EXTRA JARS
##############

RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.13/${SPARK_VERSION}/spark-connect_2.13-${SPARK_VERSION}.jar \
    -o $SPARK_HOME/jars/spark-connect_2.13-${SPARK_VERSION}.jar \
    && chown root:root $SPARK_HOME/jars/spark-connect_2.13-${SPARK_VERSION}.jar \
    && chmod 644 $SPARK_HOME/jars/spark-connect_2.13-${SPARK_VERSION}.jar

ARG NESSIE_VERSION=0.105.7
ARG AWS_SDK_VERSION=2.40.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.10.0
ARG SCALA_VERSION=2.13
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

##############
### PYTHON SETUP
##############

COPY --from=py_builder --chown=root:root /opt/python /opt/python
COPY --from=py_builder --chown=root:root /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python
