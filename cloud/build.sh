#!/usr/bin/env bash
cp -r ../build/bin compile/
cp -r ../build/lib compile/
docker build -t vearch/vearch:0.3.1 .
rm -rf compile/bin
rm -rf compile/lib