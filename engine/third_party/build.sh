#!/bin/bash

ROOT=$(dirname "$PWD")

if [ ! -d "faiss" ]; then
  FAISS_HOME=$ROOT/third_party/faiss
  tar -xzvf faiss-1.7.1.tar.gz
  mv faiss faiss-1.7.1
  pushd faiss-1.7.1
  if [ -z $MKLROOT ]; then
    cmake -DFAISS_ENABLE_GPU=$1 -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -D CMAKE_INSTALL_PREFIX=$FAISS_HOME -B build .
  else
    cmake -DFAISS_ENABLE_GPU=$1 -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -D CMAKE_INSTALL_PREFIX=$FAISS_HOME -DBLA_VENDOR=Intel10_64_dyn -DMKL_LIBRARIES=$MKLROOT/lib/intel64 -B build .
  fi
  make -C build -j2 faiss && make -C build install
  popd
  \rm -rf faiss-1.7.1
fi

