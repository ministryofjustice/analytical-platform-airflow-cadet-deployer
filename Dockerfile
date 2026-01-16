#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container
#checkov:skip=CKV_DOCKER_3: USER is set in the base image (https://github.com/ministryofjustice/analytical-platform-airflow-python-base/blob/main/Dockerfile#L135)

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.23.0@sha256:7455d1712e9671b8dabc8a37e5576ca890509ebdfdc2399a0baf6dd2d1dcaa96

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

WORKDIR ${ANALYTICAL_PLATFORM_DIRECTORY}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
