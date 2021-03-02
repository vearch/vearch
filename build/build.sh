#!/bin/bash

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin/
mkdir -p $BUILDOUT
GAMMAOUT=$ROOT/build/gamma_build
rm -rf ${GAMMAOUT}
mkdir -p $GAMMAOUT

ZFP_URL=https://github.com/LLNL/zfp/archive/0.5.5.tar.gz
FAISS_URL=https://github.com/facebookresearch/faiss/archive/v1.6.4.tar.gz
ROCKSDB_URL=https://github.com/facebook/rocksdb/archive/v6.2.2.tar.gz

# version value
BUILD_VERSION="3.2.5"

use_zfp="y"
use_rocksdb="y"
while [ -z $use_zfp ] || ([ $use_zfp != "y" ] && [ $use_zfp != "n" ])
do
  echo "Do you use zfp?[y/n]."
  read use_zfp
done

while [ -z $use_rocksdb ] || ([ $use_rocksdb != "y" ] && [ $use_rocksdb != "n" ])
do
  echo "Do you use rocksdb?[y/n]."
  read  use_rocksdb
done

if [ $use_zfp == "y" ] && [ ! -n "${ZFP_HOME}" ]; then
  export ZFP_HOME=/usr/local/include/
  rm -rf zfp*
  wget ${ZFP_URL} -O zfp.tar.gz
  tar -xzvf zfp.tar.gz
  pushd zfp* 
  mkdir build && cd build
  cmake ..
  cmake --build . --config Release
  make install
  popd
  rm -rf zfp*
fi



if [ ! -n "${FAISS_HOME}" ]; then
  export FAISS_HOME=$ROOT/build/lib/faiss
  if [ ! -d "${FAISS_HOME}" ]; then
    rm -rf faiss*
    curl -Lk ${FAISS_URL} -o faiss.tar.gz
    tar -xzvf faiss.tar.gz
    pushd faiss*
    cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DCMAKE_INSTALL_PREFIX=${FAISS_HOME} .
    make -C build && make -C build install
    popd
    rm -rf faiss*
  fi
fi

OS_NAME=$(uname)
if [ $use_rocksdb == "y" ] && [ ${OS_NAME} == "Darwin" ]; then
  export ROCKSDB_HOME=/usr/local/include/rocksdb
  brew install rocksdb
else
  if [ $use_rocksdb == "y" ] && [ ! -n "${ROCKSDB_HOME}" ]; then
    export ROCKSDB_HOME=/usr/local/include/rocksdb
    if [ ! -d "${ROCKSDB_HOME}" ]; then
      rm -rf rocksdb*
      wget  ${ROCKSDB_URL} -O rocksdb.tar.gz
      tar -xzvf rocksdb.tar.gz
      pushd rocksdb*
      CFLAGS="-O3 -fPIC" make shared_lib -j
      make install
      popd
      rm -rf rocksdb*
    fi
  fi
fi

flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"

echo "version info: $flags"

echo "build gamma"
pushd $GAMMAOUT
cmake -DPERFORMANCE_TESTING=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$ROOT/ps/engine/gammacb/lib $ROOT/engine/
make -j && make install
popd

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROOT/ps/engine/gammacb/lib/lib/
export LIBRARY_PATH=$LIBRARY_PATH:$ROOT/ps/engine/gammacb/lib/lib/

echo "build vearch"
go build -a -tags="vector" -ldflags "$flags" -o $BUILDOUT/vearch $ROOT/startup.go

echo "build deploy tool"
go build -a -ldflags "$flags" -o $BUILDOUT/batch_deployment $ROOT/tools/deployment/batch_deployment.go
