#!/usr/bin/env bash

# will download gamma in /engine with correct version
git submodule init
git submodule update 

docker run -v $(dirname "$PWD"):/vearch vearch/vearch_env:0.3.1 /vearch/cloud/compile/compile.sh
