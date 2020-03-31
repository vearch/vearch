#!/usr/bin/env bash

docker run -v $(dirname "$PWD"):/vearch vearch_env:0.3 /vearch/cloud/compile/compile.sh
