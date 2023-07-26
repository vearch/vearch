#!/usr/bin/env bash
version=latest

if [ $# -ge 1 ]; then
    version=$1
fi
echo "Build version: "$version

echo "Build compile Environment"
./compile_env.sh $version

echo "Compile Vearch"
./compile.sh $version

echo "Make Vearch Image"
./build.sh $version

echo "Start service by all in one model"
cat ../config/config.toml.example > config.toml
nohup docker run --privileged -p 8817:8817 -p 9001:9001 -v $PWD/config.toml:/vearch/config.toml vearch/vearch:$version all &

echo "good luck service is ready you can visit http://127.0.0.1:9001 to use it"
