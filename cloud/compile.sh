#!/usr/bin/env bash

docker run -v $(dirname "$PWD"):/vearch  ansj/vearch_env /vearch/cloud/compile/compile.sh
