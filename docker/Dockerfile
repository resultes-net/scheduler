FROM python:3.12-slim-bookworm AS pip-dependencies

WORKDIR /app

COPY docker-utils docker-utils
COPY requirements/ requirements/

RUN docker-utils/pip-install.sh --upgrade wheel uv pip
RUN docker-utils/uv-pip-install.sh -r requirements/run-3rd-party.txt

FROM pip-dependencies

COPY pydantic-models pydantic-models
RUN docker-utils/uv-pip-install.sh -r requirements/run.txt

COPY src src
WORKDIR /app/src

ENTRYPOINT ["python", "main.py"]
