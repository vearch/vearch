#!/usr/bin/env bash

docker run -v $(dirname "$PWD"):/vearch vearch/vearch_env:0.3.1 /vearch/cloud/compile/compile.sh
