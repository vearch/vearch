#!/usr/bin/env bash

ARCH=$(arch)

if [ ! -d "/env/app" ]; then
    mkdir -p /env/app
fi
cd /env/app/

wget -q https://github.com/protocolbuffers/protobuf/releases/download/v21.0/protobuf-cpp-3.21.0.tar.gz
tar xf protobuf-cpp-3.21.0.tar.gz
cd protobuf-3.21.0
./configure && make -j4 && make install

cd /env/app
if [ ! -f "rocksdb-v9.2.1.tar.gz" ]; then
    wget -q https://github.com/facebook/rocksdb/archive/refs/tags/v9.2.1.tar.gz -O rocksdb.tar.gz
fi
tar xf rocksdb.tar.gz
cd /env/app/rocksdb-9.2.1
sed -i '/CFLAGS += -g/d' Makefile
sed -i '/CXXFLAGS += -g/d' Makefile
CFLAGS="-O3 -fPIC" CXXFLAGS="-O3 -fPIC" make static_lib -j4
mkdir -p /env/app/rocksdb_install/lib
cp librocksdb.a /env/app/rocksdb_install/lib
cp -r /env/app/rocksdb-9.2.1/include /env/app/rocksdb_install/
rm -rf /env/app/rocksdb-9.2.1 /env/app/rocksdb.tar.gz
