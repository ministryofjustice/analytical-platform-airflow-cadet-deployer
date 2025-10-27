#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container
#checkov:skip=CKV_DOCKER_3: USER is set in the base image (https://github.com/ministryofjustice/analytical-platform-airflow-python-base/blob/main/Dockerfile#L135)

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.20.0@sha256:6cfccc9aca038a56a0400a8b382f989ed7ba6868f35e0d94fe564cee3f2e6cd5

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
