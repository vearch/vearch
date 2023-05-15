#!/usr/bin/env bash

docker run --privileged -i -v $(dirname "$PWD"):/vearch vearch/vearch_env:latest /vearch/cloud/compile/compile.sh
