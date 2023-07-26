#!/usr/bin/env bash
version=latest

if [ $# -ge 1 ]; then
    version=$1
fi

docker run --privileged -i -v $(dirname "$PWD"):/vearch vearch/vearch_env:latest /vearch/cloud/compile/compile.sh $version
