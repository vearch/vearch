#!/usr/bin/env bash

yum update
yum install -y epel-release
yum install -y wget make automake git blas-devel lapack-devel which libzstd-devel openssl-devel openblas-devel tbb-devel boost-devel

yum -y install centos-release-scl
yum -y install devtoolset-10
source /opt/rh/devtoolset-10/enable
g++ -v

ARCH=$(arch)

if [ ! -d "/env/app" ]; then
    mkdir -p /env/app
fi
cd /env/app/

CMAKE_FILE=""

if [ $ARCH = "x86_64" ]; then
    CMAKE_FILE=cmake-3.20.0-linux-x86_64
elif [ $ARCH = "aarch64" ]; then
    CMAKE_FILE=cmake-3.20.0-linux-aarch64
fi

wget https://github.com/Kitware/CMake/releases/download/v3.20.0/${CMAKE_FILE}.sh
bash ${CMAKE_FILE}.sh --skip-license --prefix=/usr/local
cp -r -p /usr/local/${CMAKE_FILE}/bin/* /bin/
rm -rf ${CMAKE_FILE}.sh

wget https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protobuf-cpp-3.6.1.tar.gz
tar xf protobuf-cpp-3.6.1.tar.gz
cd protobuf-3.6.1
./configure && make && make install

cd /env/app
if [ ! -f "rocksdb-v9.2.1.tar.gz" ]; then
    wget https://github.com/facebook/rocksdb/archive/refs/tags/v9.2.1.tar.gz -O rocksdb.tar.gz
fi
tar xf rocksdb.tar.gz
cd /env/app/rocksdb-9.2.1
make shared_lib -j4
mkdir -p /env/app/rocksdb_install/lib
cp librocksdb.so.9.2.1 /env/app/rocksdb_install/lib
cd /env/app/rocksdb_install/lib
ln -s librocksdb.so.9.2.1 librocksdb.so.9.2
ln -s librocksdb.so.9.2 librocksdb.so
cp -r /env/app/rocksdb-9.2.1/include /env/app/rocksdb_install/
rm -rf /env/app/rocksdb-9.2.1 /env/app/rocksdb.tar.gz

cd /env/app/

if [ $ARCH = "x86_64" ]; then
    if [ ! -f "go1.22.3.linux-amd64.tar.gz" ]; then
        wget https://go.dev/dl/go1.22.3.linux-amd64.tar.gz
    fi
    tar -xzf go1.22.3.linux-amd64.tar.gz
    rm -rf go1.22.3.linux-amd64.tar.gz
elif [ $ARCH = "aarch64" ]; then
    if [ ! -f "go1.22.3.linux-arm64.tar.gz" ]; then
        wget https://go.dev/dl/go1.22.3.linux-arm64.tar.gz
    fi
    tar -xzf go1.22.3.linux-arm64.tar.gz
    rm -rf go1.22.3.linux-arm64.tar.gz
fi
