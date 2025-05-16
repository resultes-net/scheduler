#!/bin/env bash
ADDITIONAL_PROPERTIES="packageName=generated_client"
rm -rf generated_client npx openapi-generator-cli generate -i openapi-schema/internal-server-openapi.json -o generated_client -g python --additional-properties=$ADDITIONAL_PROPERTIES
