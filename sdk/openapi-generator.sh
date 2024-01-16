#!/bin/sh

rm -rf go && mkdir go && openapi-generator generate -i openapi.yml -g go -o go --additional-properties=packageName=vearch_client --git-user-id="vearch" --git-repo-id="vearch/sdk/go"
# rm -rf python/* && openapi-generator generate -i openapi.yml -g python -o python --additional-properties=packageName=vearch_client --git-user-id="vearch" --git-repo-id="vearch/sdk/python"
# rm -rf java/* && openapi-generator generate -i openapi.yml -g java -o java --additional-properties=packageName=vearch_client --git-user-id="vearch" --git-repo-id="vearch/sdk/java"
