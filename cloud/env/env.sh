#!/usr/bin/env bash

yum update
yum install -y wget gcc gcc-c++ make automake git blas-devel lapack-devel

cd /env/app/
if [ ! -f "cmake-3.12.4.tar.gz" ]; then
    wget https://cmake.org/files/v3.12/cmake-3.12.4.tar.gz
fi
tar -xzf cmake-3.12.4.tar.gz
cd /env/app/cmake-3.12.4
./bootstrap
gmake
gmake install


cd /env/app/
if [ ! -d "faiss" ]; then
    git fetch https://github.com/facebookresearch/faiss.git v1.5.3
fi
cd /env/app/faiss
./configure --without-cuda --prefix=/env/app/faiss_install
make
make install


cd /env/app/
if [ ! -f "go1.12.7.linux-amd64.tar.gz" ]; then
    wget https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz
fi
tar -xzf go1.12.7.linux-amd64.tar.gz

