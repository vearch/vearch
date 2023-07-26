#!/usr/bin/env bash
version=latest

if [ $# -ge 1 ]; then
    version=$1
fi

cp -r ../build/bin compile/
cp -r ../build/lib compile/
docker build -t vearch/vearch:$version .
rm -rf compile/bin
rm -rf compile/lib
