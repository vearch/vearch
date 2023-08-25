#!/bin/bash

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin/
mkdir -p $BUILDOUT
GAMMAOUT=$ROOT/build/gamma_build

# BUILD OPTS
COMPILE_THREAD_TAG=-j2
BUILD_GAMMA_TEST=OFF
BUILD_GAMMA_TYPE=Release

# version value
BUILD_VERSION="latest"

while getopts ":n:tdh" opt
do
  case $opt in
    n)
      COMPILE_THREAD_NUM="-j"$OPTARG
      echo "COMPILE_THREAD_NUM="$COMPILE_THREAD_NUM;;
    t)
      BUILD_GAMMA_TEST=ON
      echo "BUILD_GAMMA_TEST=ON";;
    d)
      BUILD_GAMMA_TYPE=Debug
      echo "BUILD_GAMMA_TYPE="$BUILD_GAMMA_TYPE;;
    h)
      echo "[build options]"
      echo -e "\t-h\t\thelp"
      echo -e "\t-n\t\tcompile thread num"
      echo -e "\t-t\t\tbuild gamma test"
      echo -e "\t-d\t\tbuild gamma type=Debug"
      exit 0;;
    ?)
      echo "unsupport param, -h for help"
      exit 1;;
  esac
done

ZFP_URL=https://github.com/LLNL/zfp/archive/0.5.5.tar.gz
ROCKSDB_URL=https://github.com/facebook/rocksdb/archive/refs/tags/v6.6.4.tar.gz

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
  read use_rocksdb
done

function get_version() {
  VEARCH_VERSION_MAJOR=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_MINOR=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_PATCH=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}'`

  BUILD_VERSION="v${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  echo "BUILD_VERSION="${BUILD_VERSION}
}

function build_thirdparty() {
  if [ $use_zfp == "y" ] && [ ! -n "${ZFP_HOME}" ]; then
    export ZFP_HOME=/usr/local/include/zfp
    if [ ! -d $ZFP_HOME ]; then
      rm -rf zfp*
      wget ${ZFP_URL} -O zfp.tar.gz
      tar -xzf zfp.tar.gz
      pushd zfp-0.5.5
      mkdir build && cd build
      cmake ..
      cmake --build . --config Release
      make install
      popd
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
        tar -xzf rocksdb.tar.gz
        pushd rocksdb-6.6.4
        CFLAGS="-O3 -fPIC" make shared_lib $COMPILE_THREAD_TAG
        make install
        popd
      fi
    fi
  fi
}

function build_engine() {
  echo "build gamma"
  rm -rf ${GAMMAOUT} && mkdir -p $GAMMAOUT
  pushd $GAMMAOUT
  cmake -DPERFORMANCE_TESTING=ON -DCMAKE_BUILD_TYPE=$BUILD_GAMMA_TYPE -DBUILD_TEST=$BUILD_GAMMA_TEST $ROOT/engine/
  make $COMPILE_THREAD_TAG
  popd
}

function build_vearch() {
  flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"
  echo "version info: $flags"
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GAMMAOUT
  export LIBRARY_PATH=$LIBRARY_PATH:$GAMMAOUT

  echo "build vearch"
  go build -a -tags="vector" -ldflags "$flags" -o $BUILDOUT/vearch $ROOT/startup.go
  echo "build deploy tool"
  go build -a -ldflags "$flags" -o $BUILDOUT/batch_deployment $ROOT/tools/deployment/batch_deployment.go
}

get_version
build_thirdparty
build_engine
build_vearch
