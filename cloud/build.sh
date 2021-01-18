#!/usr/bin/env bash
cp -r ../build/bin compile/
cp -r ../build/lib compile/
docker build -t vearch/vearch:3.2.6 .
rm -rf compile/bin
rm -rf compile/lib