#!/bin/bash

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin/
mkdir -p $BUILDOUT
GAMMAOUT=$ROOT/build/gamma_build

# BUILD OPTS
COMPILE_THREAD_NUM=-j4
BUILD_GAMMA=ON
BUILD_GAMMA_TEST=OFF
BUILD_GAMMA_TYPE=Release
BUILD_GAMMA_OPT_LEVEL=avx512

# version value
BUILD_VERSION="latest"

while getopts ":n:g:tdh" opt; do
  case $opt in
  n)
    COMPILE_THREAD_NUM="-j"$OPTARG
    echo "COMPILE_THREAD_NUM="$COMPILE_THREAD_NUM
    ;;
  t)
    BUILD_GAMMA_TEST=ON
    echo "BUILD_GAMMA_TEST=ON"
    ;;
  d)
    BUILD_GAMMA_TYPE=Debug
    echo "BUILD_GAMMA_TYPE="$BUILD_GAMMA_TYPE
    ;;
  g)
    BUILD_GAMMA=$OPTARG
    echo "BUILD_GAMMA="$BUILD_GAMMA
    ;;
  o)
    BUILD_GAMMA_OPT_LEVEL=$OPTARG
    echo "BUILD_GAMMA_OPT_LEVEL="$BUILD_GAMMA_OPT_LEVEL
    ;;
  h)
    echo "[build options]"
    echo -e "\t-h\t\thelp"
    echo -e "\t-n\t\tcompile thread num"
    echo -e "\t-g\t\tbuild gamma or not: [ON|OFF]"
    echo -e "\t-t\t\tbuild gamma test"
    echo -e "\t-d\t\tbuild gamma type=Debug"
    echo -e "\t-o\t\tbuild gamma opt level=[generic|avx2|avx512]"
    exit 0
    ;;
  ?)
    echo "unsupport param, -h for help"
    exit 1
    ;;
  esac
done

ROCKSDB_URL=https://github.com/facebook/rocksdb/archive/refs/tags/v9.2.1.tar.gz

function get_version() {
  VEARCH_VERSION_MAJOR=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}')
  VEARCH_VERSION_MINOR=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}')
  VEARCH_VERSION_PATCH=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}')

  BUILD_VERSION="v${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  echo "BUILD_VERSION="${BUILD_VERSION}
}

function build_thirdparty() {
  if [ ! -n "${ROCKSDB_HOME}" ]; then
    export ROCKSDB_HOME=/usr/local/include/rocksdb
    if [ ! -d "${ROCKSDB_HOME}" ]; then
      rm -rf rocksdb*
      wget -q ${ROCKSDB_URL} -O rocksdb.tar.gz
      tar -xzf rocksdb.tar.gz
      pushd rocksdb-9.2.1
      sed -i '/CFLAGS += -g/d' Makefile
      sed -i '/CXXFLAGS += -g/d' Makefile
      CFLAGS="-O3 -fPIC" CXXFLAGS="-O3 -fPIC" make static_lib $COMPILE_THREAD_NUM
      make install
      popd
    fi
  fi

  if [[ ! -f "/usr/local/lib/libprotobuf.so" ]]; then
    wget -q https://github.com/protocolbuffers/protobuf/releases/download/v21.0/protobuf-cpp-3.21.0.tar.gz
    tar xf protobuf-cpp-3.21.0.tar.gz
    pushd protobuf-3.21.0
    ./configure && make -j4 && make install
    popd
  fi

  if [[ ! -f "/usr/local/lib64/libroaring.a" ]]; then
    wget -q https://github.com/RoaringBitmap/CRoaring/archive/refs/tags/v4.2.1.tar.gz
    tar xf v4.2.1.tar.gz
    pushd CRoaring-4.2.1
    mkdir build && pushd build
    cmake ../ -B ./ -DCMAKE_CXX_STANDARD=17 -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    make -j4 && make install
    popd && popd
  fi

  if [ ! -n "${FAISS_HOME}" ]; then
    if [[ ! -f "/usr/local/lib64/libfaiss.a" ]]; then
      wget -q https://github.com/facebookresearch/faiss/archive/refs/tags/v1.10.0.tar.gz
      tar xf v1.10.0.tar.gz
      pushd faiss-1.10.0
      if [ -z $MKLROOT ]; then
        OS_NAME=$(uname)
        ARCH=$(arch)
        if [ ${OS_NAME} == "Darwin" ]; then
          cmake -DFAISS_ENABLE_GPU=OFF -DOpenMP_CXX_FLAGS="-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include" -DOpenMP_CXX_LIB_NAMES="libomp" -DOpenMP_libomp_LIBRARY="/usr/local/opt/libomp/lib" -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx2 -B build .
        elif [ ${ARCH} == "aarch64" -o ${ARCH} == "AARCH64" ]; then
          cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -B build .
        else
          cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx512 -B build .
        fi
      else
        cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_OPT_LEVEL=avx512 -DBLA_VENDOR=Intel10_64_dyn -DMKL_LIBRARIES=$MKLROOT/lib/intel64 -B build .
      fi

      make -C build faiss && make -C build install
      popd
    fi
  fi

}

function build_engine() {
  echo "build gamma"
  rm -rf ${GAMMAOUT} && mkdir -p $GAMMAOUT
  pushd $GAMMAOUT
  cmake -DPERFORMANCE_TESTING=ON -DCMAKE_BUILD_TYPE=$BUILD_GAMMA_TYPE -DBUILD_TEST=$BUILD_GAMMA_TEST -DBUILD_THREAD_NUM=$COMPILE_THREAD_NUM -DBUILD_GAMMA_OPT_LEVEL=$BUILD_GAMMA_OPT_LEVEL $ROOT/internal/engine/
  make $COMPILE_THREAD_NUM
  popd
}

function build_vearch() {
  flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"
  echo "version info: $flags"
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GAMMAOUT
  export LIBRARY_PATH=$LIBRARY_PATH:$GAMMAOUT

  echo "build vearch"
  go build -a -tags="vector" -ldflags "$flags" -o $BUILDOUT/vearch $ROOT/cmd/vearch/startup.go
}

get_version
if [ $BUILD_GAMMA == "ON" ]; then
  build_thirdparty
  build_engine
fi
build_vearch
