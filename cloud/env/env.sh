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
cd /env/app

if [[ ! -f "/usr/local/lib64/libroaring.a" ]]; then
    wget -q https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz
    tar xf v4.2.1.tar.gz
    pushd CRoaring-4.2.1
    mkdir build && pushd build
    cmake ../ -B ./ -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    make -j4 && make install
    popd && popd
fi

if [[ ! -f "/usr/local/lib64/libfaiss.a" ]]; then
    wget -q https://github.com/facebookresearch/faiss/archive/refs/tags/v1.7.1.tar.gz
    tar xf v1.7.1.tar.gz
    pushd faiss-1.7.1
    if [ -z $MKLROOT ]; then
        OS_NAME=$(uname)
        ARCH=$(arch)
        if [ ${OS_NAME} == "Darwin" ]; then
            cmake -DFAISS_ENABLE_GPU=OFF -DOpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include" -DOpenMP_CXX_LIB_NAMES="libomp" -DOpenMP_libomp_LIBRARY="/usr/local/opt/libomp/lib" -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -B build .
        elif [ ${ARCH} == "aarch64" -o ${ARCH} == "AARCH64" ]; then
            cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -B build .
        else
            cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -B build .
        fi
    else
        cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -DBLA_VENDOR=Intel10_64_dyn -DMKL_LIBRARIES=$MKLROOT/lib/intel64 -B build .
    fi

    make -C build faiss && make -C build install
    popd
fi
