FROM python:3.12-slim-bookworm AS pip-dependencies

WORKDIR /app

COPY docker-utils docker-utils
COPY requirements/ requirements/

RUN docker-utils/pip-install.sh --upgrade wheel uv pip
RUN docker-utils/uv-pip-install.sh -r requirements/run-3rd-party.txt

FROM pip-dependencies

COPY pydantic-models pydantic-models
COPY openstack-utils openstack-utils
COPY jsonrpc jsonrpc
RUN docker-utils/uv-pip-install.sh -r requirements/run.txt

COPY config/clouds.yaml config/clouds.yaml

COPY src src
WORKDIR /app/src

ENTRYPOINT ["python", "main.py"]
