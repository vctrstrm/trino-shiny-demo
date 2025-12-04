ARG TRINO_VERSION=475

FROM alpine:3 AS builder
ARG VAST_TRINO_PLUGIN_ZIP_URL="https://github.com/vast-data/vast-db-connectors/releases/download/trino-vast-5.4.0-475-e330d19e3523/trino-vast-5.4.0-475-e330d19e3523.zip"

RUN \
  echo "[BUILD] Downloading/Unpacking VAST DB Trino Connector JAR's" \
  && __vast_trino_tmp_dir="/tmp/vast" \
  && __vast_trino_tmp_zip_file="${__vast_trino_tmp_dir}/vast-trino-jars.zip" \
  && __vast_trino_plugin_dir="/vast-trino-plugin" \
  && mkdir -p "${__vast_trino_tmp_dir}" "${__vast_trino_plugin_dir}" \
  && wget -O "${__vast_trino_tmp_zip_file}" "${VAST_TRINO_PLUGIN_ZIP_URL}" \
  && unzip "${__vast_trino_tmp_zip_file}" -d "${__vast_trino_tmp_dir}" \
  && find "${__vast_trino_tmp_dir}" -type f -name "*.jar" -exec cp '{}' "${__vast_trino_plugin_dir}/" ';' \
  && rm -rf "${__vast_trino_tmp_dir}"

FROM trinodb/trino:${TRINO_VERSION}

ARG VAST_TRINO_PLUGIN_DIR=/usr/lib/trino/plugin/vast
ARG TRINO_IMAGE_UID=1000
ARG TRINO_IMAGE_GID=1000

USER 0
RUN \
  echo "[BUILD] Creating VAST DB Trino Connector JAR's directory" \
  && mkdir -p "${VAST_TRINO_PLUGIN_DIR}" \
  && chown ${TRINO_IMAGE_UID}:${TRINO_IMAGE_GID} "${VAST_TRINO_PLUGIN_DIR}" \
  && chmod 755 "${VAST_TRINO_PLUGIN_DIR}"
COPY --from=builder --chown=${TRINO_IMAGE_UID}:${TRINO_IMAGE_GID} --chmod=644 /vast-trino-plugin/ "${VAST_TRINO_PLUGIN_DIR}/"

USER ${TRINO_IMAGE_UID}:${TRINO_IMAGE_GID}