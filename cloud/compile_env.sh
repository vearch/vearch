#!/usr/bin/env bash

cd env
mkdir app
docker build -t vearch/vearch_env:0.3.1 .

