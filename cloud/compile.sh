#!/usr/bin/env bash

docker run -v $(dirname "$PWD"):/vearch  ansj/vearch_env:0.2 /vearch/cloud/compile/compile.sh
