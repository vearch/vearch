#!/usr/bin/env bash

#add env
export GOROOT=/env/app/go
export PATH=$PATH:$GOROOT/bin
export ROCKSDB_HOME=/env/app/rocksdb_install
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROCKSDB_HOME/lib

source /opt/rh/devtoolset-10/enable
# to compile
cd /vearch/build
mkdir -p /env/app/go/src/github.com/vearch
ln -s /vearch/ /env/app/go/src/github.com/vearch

cd /env/app/go/src/github.com/vearch/vearch/build
./build.sh

mkdir -p /vearch/build/lib/

cp /env/app/rocksdb_install/lib/librocksdb.* /vearch/build/lib/
cp /vearch/build/gamma_build/libgamma.* /vearch/build/lib/
cp /vearch/engine/third_party/faiss/lib*/* /vearch/build/lib/

rm -rf /vearch/build/gamma_build
