#!/usr/bin/env bash

docker run --privileged -it -v $(dirname "$PWD"):/vearch vearch/vearch_env:3.2.8 /vearch/cloud/compile/compile.sh
