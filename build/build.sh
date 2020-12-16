#!/bin/bash

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin/
mkdir -p $BUILDOUT
GAMMAOUT=$ROOT/build/gamma_build
rm -rf ${GAMMAOUT}
mkdir -p $GAMMAOUT

FAISS_URL=https://github.com/facebookresearch/faiss/archive/v1.6.4.tar.gz
ROCKSDB_URL=https://github.com/facebook/rocksdb/archive/v6.4.6.tar.gz

# version value
BUILD_VERSION="3.2.2"

if [ ! -n "${FAISS_HOME}" ]; then
  export FAISS_HOME=$ROOT/build/lib/faiss
  if [ ! -d "${FAISS_HOME}" ]; then
    rm -rf faiss*
    curl -Lk ${FAISS_URL} -o faiss.tar.gz
    tar xf faiss.tar.gz
    pushd faiss*
    cmake -B build -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DCMAKE_INSTALL_PREFIX=${FAISS_HOME} .
    make -C build && make -C build install
    popd
    rm -rf faiss*
  fi
fi

OS_NAME=$(uname)
if [ ${OS_NAME} == "Darwin" ]; then
  export ROCKSDB_HOME=/usr/local/include/rocksdb
  brew install rocksdb
else
  if [ ! -n "${ROCKSDB_HOME}" ]; then
    export ROCKSDB_HOME=/usr/local/include/rocksdb
    if [ ! -d "${ROCKSDB_HOME}" ]; then
      rm -rf rocksdb*
      curl -Lk ${ROCKSDB_URL} -o rocksdb.tar.gz
      tar xf rocksdb.tar.gz
      pushd rocksdb*
      CFLAGS="-O3 -fPIC" make static_lib -j
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
