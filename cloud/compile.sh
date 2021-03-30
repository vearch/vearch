#!/usr/bin/env bash

# will download gamma in /engine with correct version
git submodule init
git submodule update --remote

docker run -it -v $(dirname "$PWD"):/vearch vearch/vearch_env:3.2.6 /vearch/cloud/compile/compile.sh