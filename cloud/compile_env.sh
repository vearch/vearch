#!/usr/bin/env bash

cd env
mkdir app
docker build -t vearch_env:0.3 .

