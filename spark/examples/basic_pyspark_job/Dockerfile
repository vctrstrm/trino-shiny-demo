# =========================================================
# Builder Image
# =========================================================

FROM ghcr.io/astral-sh/uv:bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Install build essentials and required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Configure the Python directory so it is consistent
ENV UV_PYTHON_INSTALL_DIR=/python

# Only use the managed Python version
ENV UV_PYTHON_PREFERENCE=only-managed

# Install Python before the project for caching
RUN uv python install 3.12

WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# =========================================================
# Final Image
# =========================================================

FROM debian:bookworm-slim

ARG SPARK_VERSION=3.5.6
ARG JAVA_VERSION=11

# Install necessary packages for Java download and extraction
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Download and install Amazon Corretto 11 JDK
ENV JAVA_VERSION=${JAVA_VERSION}
ENV JAVA_HOME=/opt/java/amazon-corretto-${JAVA_VERSION}
RUN mkdir -p /opt/java \
    && curl -L https://corretto.aws/downloads/latest/amazon-corretto-${JAVA_VERSION}-x64-linux-jdk.tar.gz -o /tmp/jdk.tgz \
    && tar -xzf /tmp/jdk.tgz -C /opt/java \
    && ln -s /opt/java/amazon-corretto-* ${JAVA_HOME} \
    && rm /tmp/jdk.tgz

# INSTALL SPARK (because pyspark bundles scala2.12 jars and we need scala2.13)
ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
# Spark distribution name (Scala 2.13 is default for Hadoop 3 build of Spark 3.2+)
ARG SPARK_DIST_NAME="spark-${SPARK_VERSION}-bin-hadoop3-scala2.13"
ARG SPARK_TGZ_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_DIST_NAME}.tgz"

RUN curl -L "$SPARK_TGZ_URL" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    rm /tmp/spark.tgz && \
    mv "/opt/${SPARK_DIST_NAME}" "${SPARK_HOME}"

COPY --from=builder --chown=python:python /python /python
COPY --from=builder --chown=app:app /app /app

ENV PATH="/app/.venv/bin:${JAVA_HOME}/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

RUN chmod +x /app/spark-submit.sh

CMD ["/app/spark-submit.sh"]
