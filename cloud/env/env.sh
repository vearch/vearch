#!/usr/bin/env bash

yum update
yum install -y epel-release
yum install -y wget gcc gcc-c++ make automake git blas-devel lapack-devel which openssl-devel libzstd-devel
if [ ! -d "/env/app" ]; then
  mkdir -p /env/app
fi
cd /env/app/
if [ ! -f "cmake-3.12.4.tar.gz" ]; then
    wget https://cmake.org/files/v3.12/cmake-3.12.4.tar.gz
fi
tar -xzf cmake-3.12.4.tar.gz
cd /env/app/cmake-3.12.4
./bootstrap
gmake
gmake install
cd /usr/bin
if [ ! -f "cmake" ]; then
    ln -s cmake3 cmake
fi
cd /env/app/
if [ ! -d "faiss" ]; then
    git clone https://github.com/facebookresearch/faiss.git
    cd faiss
    git reset --hard 2a36d4d8c7c92fccb7d93f2da21ed0196483dee0
    rm -rf gpu
fi
cd /env/app/faiss
./configure --without-cuda --prefix=/env/app/faiss_install
make -j && make install

cd /env/app
if [ ! -f "rocksdb-v6.2.2.tar.gz" ]; then
    wget https://github.com/facebook/rocksdb/archive/v6.2.2.tar.gz -O rocksdb-v6.2.2.tar.gz
fi
tar -xzf rocksdb-v6.2.2.tar.gz
cd /env/app/rocksdb-6.2.2
make shared_lib
mkdir -p /env/app/rocksdb_install/lib
cp librocksdb.so.6.2.2 /env/app/rocksdb_install/lib
cd /env/app/rocksdb_install/lib
ln -s librocksdb.so.6.2.2 librocksdb.so.6.2
ln -s librocksdb.so.6.2 librocksdb.so
cp -r /env/app/rocksdb-6.2.2/include /env/app/rocksdb_install/

cd /env/app/
if [ ! -f "go1.12.7.linux-amd64.tar.gz" ]; then
    wget https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz
fi
tar -xzf go1.12.7.linux-amd64.tar.gz

