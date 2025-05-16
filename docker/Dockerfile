FROM node:23.11-bookworm-slim AS npm-dependencies

WORKDIR /app

# Install Java for `openapischema-generator-cli`.
RUN apt-get update
RUN apt-get install -y openjdk-17-jre-headless

COPY package.json package-lock.json ./

RUN npm ci


FROM npm-dependencies AS generated-client

COPY openapi-schema openapi-schema
COPY scripts/generate-client.sh scripts/generate-client.sh
RUN ./scripts/generate-client.sh

FROM python:3.12-slim-bookworm

RUN python -m pip install --upgrade wheel pip uv

COPY requirements/run-3rd-party.txt requirements/run-3rd-party.txt 
COPY requirements/run.txt requirements/run.txt

COPY src src
COPY pyproject.toml .

COPY --from=generated-client /app/generated_client generated_client

RUN python -m uv pip install -r requirements/run.txt

ENTRYPOINT ["python", "src/resultes_scheduler/main.py"]
