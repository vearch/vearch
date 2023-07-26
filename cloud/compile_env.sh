#!/usr/bin/env bash
version=latest

if [ $# -ge 1 ]; then
    version=$1
fi

cd env
docker build -t vearch/vearch_env:$version .
