#checkov:skip=CKV_DOCKER_2: HEALTHCHECK not required - This is a utility container
#checkov:skip=CKV_DOCKER_3: USER is set in the base image (https://github.com/ministryofjustice/analytical-platform-airflow-python-base/blob/main/Dockerfile#L135)

FROM ghcr.io/ministryofjustice/analytical-platform-airflow-python-base:1.24.1@sha256:ce3298a43c74b6811ee7d60d8dc44cced167f92570f027e2aff911ed096ce1f7

ARG MOJAP_IMAGE_VERSION="default"
ENV MOJAP_IMAGE_VERSION=${MOJAP_IMAGE_VERSION}

WORKDIR ${ANALYTICAL_PLATFORM_DIRECTORY}

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY --chown=${CONTAINER_UID}:${CONTAINER_GID} --chmod=0755 src/ ${ANALYTICAL_PLATFORM_DIRECTORY}

ENTRYPOINT ["./entrypoint.sh"]
