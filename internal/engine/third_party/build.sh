#!/bin/bash

ROOT=$(dirname "$PWD")

if [ ! -d "faiss" ]; then
  FAISS_HOME=$ROOT/third_party/faiss
  tar -xzf faiss-1.7.1.tar.gz
  pushd faiss-1.7.1
  if [ -z $MKLROOT ]; then
    OS_NAME=$(uname)
    if [ ${OS_NAME} == "Darwin" ]; then
      cmake -DOpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include" -DOpenMP_CXX_LIB_NAMES="libomp" -DOpenMP_libomp_LIBRARY="/usr/local/opt/libomp/lib" -DFAISS_ENABLE_GPU=$1 -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -D CMAKE_INSTALL_PREFIX=$FAISS_HOME -B build .
    else
      cmake -DFAISS_ENABLE_GPU=$1 -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -D CMAKE_INSTALL_PREFIX=$FAISS_HOME -B build .
    fi
  else
    cmake -DFAISS_ENABLE_GPU=$1 -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -D CMAKE_INSTALL_PREFIX=$FAISS_HOME -DBLA_VENDOR=Intel10_64_dyn -DMKL_LIBRARIES=$MKLROOT/lib/intel64 -B build .
  fi

  COMPILE_THREAD_NUM=$2
  echo "COMPILE_THREAD_NUM="$COMPILE_THREAD_NUM

  make -C build $COMPILE_THREAD_NUM faiss && make -C build install
  popd
  \rm -rf faiss-1.7.1
fi
