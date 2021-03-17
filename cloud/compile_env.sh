#!/usr/bin/env bash

cd env
mkdir app
docker build -t vearch/vearch_env:3.2.6 .
