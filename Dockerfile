#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.16.0@sha256:96551185eef8ff5f54f82b6a55c5df570a795d155851cde1811f230423efca1b

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
