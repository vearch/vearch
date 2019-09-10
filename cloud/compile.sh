#!/usr/bin/env bash

docker run -it -v $(dirname "$PWD"):/vearch vearch_env /vearch/cloud/compile/compile.sh
