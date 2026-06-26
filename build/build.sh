#!/bin/bash
set -e

ROOT=$(dirname "$PWD")
BUILDOUT=$ROOT/build/bin
LIBOUT=$ROOT/build/lib
mkdir -p $BUILDOUT $LIBOUT
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

function get_version() {
  VEARCH_VERSION_MAJOR=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}')
  VEARCH_VERSION_MINOR=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}')
  VEARCH_VERSION_PATCH=$(cat ${ROOT}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}')

  BUILD_VERSION="v${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  echo "BUILD_VERSION="${BUILD_VERSION}
}

function build_engine() {
  echo "build gamma"
  local BUILD_ARCH=$(arch)
  if [[ "$BUILD_ARCH" == "aarch64" || "$BUILD_ARCH" == "AARCH64" ]]; then
    echo "DiskANN is not supported on arm64, building without DiskANN."
    BUILD_WITH_DISKANN=OFF
  else
    DISKANN_ROOT=${DISKANN_ROOT:-/usr/local}
    DISKANN_INCLUDE_DIR=${DISKANN_INCLUDE_DIR:-${DISKANN_ROOT}/include}
    DISKANN_LIBRARY=${DISKANN_LIBRARY:-}
    if [ -z "${DISKANN_LIBRARY}" ]; then
      if [ -f "${DISKANN_ROOT}/lib64/libdiskann.so" ]; then
        DISKANN_LIBRARY="${DISKANN_ROOT}/lib64/libdiskann.so"
      elif [ -f "${DISKANN_ROOT}/lib/libdiskann.so" ]; then
        DISKANN_LIBRARY="${DISKANN_ROOT}/lib/libdiskann.so"
      elif [ -f "${DISKANN_ROOT}/lib64/libdiskann.a" ]; then
        DISKANN_LIBRARY="${DISKANN_ROOT}/lib64/libdiskann.a"
      elif [ -f "${DISKANN_ROOT}/lib/libdiskann.a" ]; then
        DISKANN_LIBRARY="${DISKANN_ROOT}/lib/libdiskann.a"
      else
        echo "error: no DiskANN library found under DISKANN_ROOT=${DISKANN_ROOT} (tried lib64/lib libdiskann.{so,a} and lib/libdiskann.{so,a}). Set DISKANN_LIBRARY to the full path of libdiskann.so or libdiskann.a." >&2
        exit 1
      fi
    fi
    oneapi_root="${ONEAPI_ROOT:-/opt/intel/oneapi}"
    MKL_ROOT="${MKL_ROOT:-${oneapi_root}/mkl/latest}"
    OMP_LIB_PATH="${OMP_LIB_PATH:-${oneapi_root}/compiler/latest/lib}"
    INTEL_LIB_PATHS="${MKL_ROOT}/lib/intel64:${OMP_LIB_PATH}"
    export MKLROOT="${MKL_ROOT}"
    export LIBRARY_PATH=${INTEL_LIB_PATHS}:$LIBRARY_PATH
    export LD_LIBRARY_PATH=${INTEL_LIB_PATHS}:$LD_LIBRARY_PATH
    BUILD_WITH_DISKANN=ON
  fi
  rm -rf ${GAMMAOUT} && mkdir -p $GAMMAOUT
  pushd $GAMMAOUT
  local cmake_opts=(
    -DPERFORMANCE_TESTING=ON
    -DCMAKE_BUILD_TYPE=$BUILD_GAMMA_TYPE
    -DBUILD_TEST=$BUILD_GAMMA_TEST
    -DBUILD_THREAD_NUM=$COMPILE_THREAD_NUM
    -DBUILD_GAMMA_OPT_LEVEL=$BUILD_GAMMA_OPT_LEVEL
    -DBUILD_WITH_DISKANN=$BUILD_WITH_DISKANN
  )
  if [[ "$BUILD_WITH_DISKANN" == "ON" ]]; then
    cmake_opts+=(
      -DDISKANN_ROOT=$DISKANN_ROOT
      -DDISKANN_INCLUDE_DIR=$DISKANN_INCLUDE_DIR
      -DDISKANN_LIBRARY=$DISKANN_LIBRARY
    )
    local rpath="\$ORIGIN;${MKLROOT}/lib/intel64;${OMP_LIB_PATH}"
  else
    local rpath="\$ORIGIN"
  fi
  cmake "${cmake_opts[@]}" -DCMAKE_BUILD_RPATH="$rpath" -DCMAKE_INSTALL_RPATH="\$ORIGIN" -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=OFF $ROOT/internal/engine/
  make $COMPILE_THREAD_NUM
  popd
}

function build_vearch() {
  COMMIT_ID=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")
  flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=${COMMIT_ID}' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"
  echo "version info: $flags"
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$GAMMAOUT
  export LIBRARY_PATH=$LIBRARY_PATH:$GAMMAOUT
  export CGO_LDFLAGS="${CGO_LDFLAGS}"

  echo "build vearch"
  go build -a -tags="vector" -ldflags "$flags" -o $BUILDOUT/vearch $ROOT/cmd/vearch/startup.go
}

get_version
if [ $BUILD_GAMMA == "ON" ]; then
  build_engine
fi
build_vearch
