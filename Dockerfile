#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container
#checkov:skip=CKV_DOCKER_3: USER is set in the base image (https://github.com/ministryofjustice/analytical-platform-airflow-python-base/blob/main/Dockerfile#L135)

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.19.0@sha256:594f9d28b5b53ad55f96dce790774dccc93db892a167bcbd832bb9a06fea0e1b

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
