#!/usr/bin/env bash

yum update
yum install -y epel-release
yum install -y wget make automake git blas-devel lapack-devel which libzstd-devel openssl-devel openblas-devel tbb-devel boost-devel net-tools

yum -y install centos-release-scl
yum -y install devtoolset-10
source /opt/rh/devtoolset-10/enable
g++ -v

if [ ! -d "/env/app" ]; then
    mkdir -p /env/app
fi
cd /env/app/
if [ ! -f "cmake-3.20.0-rc3.tar.gz" ]; then
    wget https://cmake.org/files/v3.20/cmake-3.20.0-rc3.tar.gz
fi
tar xf cmake-3.20.0-rc3.tar.gz
cd /env/app/cmake-3.20.0-rc3
./bootstrap
gmake -j4 && gmake install
rm -rf /env/app/cmake-3.20.0-rc3 /env/app/cmake-3.20.0-rc3.tar.gz
cd /usr/bin
if [ ! -f "cmake" ]; then
    ln -s cmake3 cmake
fi

cd /env/app
if [ ! -f "rocksdb-v6.6.4.tar.gz" ]; then
    wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.6.4.tar.gz -O rocksdb.tar.gz
fi
tar xf rocksdb.tar.gz
cd /env/app/rocksdb-6.6.4
make shared_lib -j4
mkdir -p /env/app/rocksdb_install/lib
cp librocksdb.so.6.6.4 /env/app/rocksdb_install/lib
cd /env/app/rocksdb_install/lib
ln -s librocksdb.so.6.6.4 librocksdb.so.6.6
ln -s librocksdb.so.6.6 librocksdb.so
cp -r /env/app/rocksdb-6.6.4/include /env/app/rocksdb_install/
rm -rf /env/app/rocksdb-6.6.4 /env/app/rocksdb.tar.gz

cd /env/app/
if [ ! -f "go1.19.9.linux-amd64.tar.gz" ]; then
    wget https://dl.google.com/go/go1.19.9.linux-amd64.tar.gz
fi

tar -xzf go1.19.9.linux-amd64.tar.gz
rm -rf go1.19.9.linux-amd64.tar.gz

# install conda
cd /env/app/
if [ ! -f "Miniconda3-py310_24.3.0-0-Linux-x86_64.sh" ]; then
    wget --quiet https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-py310_24.3.0-0-Linux-x86_64.sh
fi

sh Miniconda3-py310_24.3.0-0-Linux-x86_64.sh -b
/root/miniconda3/bin/conda init bash
source ~/.bashrc

rm -rf Miniconda3-py310_24.3.0-0-Linux-x86_64.sh

pip install requests pytest numpy --quiet
