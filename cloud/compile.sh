#!/usr/bin/env bash

docker run -v $(dirname "$PWD"):/vearch vearch_env /vearch/cloud/compile/compile.sh
