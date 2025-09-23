#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container
#checkov:skip=CKV_DOCKER_3: USER is set in the base image (https://github.com/ministryofjustice/analytical-platform-airflow-python-base/blob/main/Dockerfile#L135)

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.18.0@sha256:ad47108ca5ad7a1dd616485c972259e78aa8bca9984293d4ef96b5366e687c19

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
