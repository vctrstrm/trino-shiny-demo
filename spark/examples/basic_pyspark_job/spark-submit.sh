#!/usr/bin/env bash
set -Eeuo pipefail; shopt -s failglob

# SPARK_HOME is set by the base image
echo "Environment Variables:"
echo "--------------------------------"
echo "SPARK_HOME: $SPARK_HOME"
echo "JAVA_HOME: $JAVA_HOME"
echo "--------------------------------"
echo "Running Spark job..."

# Run the Spark job
spark-submit \
    --master spark://spark-cluster:7077 \
    --deploy-mode client \
    /app/src/basic_pyspark_job/app.py