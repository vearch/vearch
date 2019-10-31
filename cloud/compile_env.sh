#!/usr/bin/env bash

cd env
mkdir app
docker build -t ansj/vearch_env:0.2 .

