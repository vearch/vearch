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
CFLAGS="-O3 -fPIC" CXXFLAGS="-O3 -fPIC" ROCKSDB_DISABLE_BZIP=1 make static_lib -j4 && make install

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
    wget -q https://github.com/facebookresearch/faiss/archive/refs/tags/v1.14.1.tar.gz
    tar xf v1.14.1.tar.gz
    pushd faiss-1.14.1
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

    make -C build faiss -j4 && make -C build install
    popd
fi


if [[ "$ARCH" != "aarch64" && "$ARCH" != "AARCH64" ]]; then
    if [[ ! -f "/usr/local/lib64/libdiskann.so" && ! -f "/usr/local/lib/libdiskann.so" ]]; then
        DISKANN_BOOST_VERSION=${DISKANN_BOOST_VERSION:-"1.78.0"}
        if [[ -z "${DISKANN_BOOST_ROOT:-}" ]]; then
            DISKANN_BOOST_ROOT="/env/app/.deps/boost-${DISKANN_BOOST_VERSION}"
        fi
        if [[ ! -f "${DISKANN_BOOST_ROOT}/include/boost/dynamic_bitset.hpp" ]] || [[ ! -f "${DISKANN_BOOST_ROOT}/lib/libboost_program_options.so" && ! -f "${DISKANN_BOOST_ROOT}/lib/libboost_program_options.a" ]]; then
            mkdir -p /env/app/.deps
            pushd /env/app/.deps
            BOOST_TAG=${DISKANN_BOOST_VERSION//./_}
            BOOST_ARCHIVE="boost_${BOOST_TAG}.tar.gz"
            BOOST_SOURCE_DIR="boost_${BOOST_TAG}"
            BOOST_DOWNLOAD_URL="https://archives.boost.io/release/${DISKANN_BOOST_VERSION}/source/${BOOST_ARCHIVE}"
            if [[ ! -f "${BOOST_ARCHIVE}" ]]; then
                wget -q "${BOOST_DOWNLOAD_URL}" -O "${BOOST_ARCHIVE}"
            fi
            if [[ ! -d "${BOOST_SOURCE_DIR}" ]]; then
                tar xf "${BOOST_ARCHIVE}"
            fi
            pushd "${BOOST_SOURCE_DIR}"
            ./bootstrap.sh --prefix="${DISKANN_BOOST_ROOT}"
            ./b2 install -j4 --with-program_options link=shared,static cxxflags=-fPIC
            popd
            popd
        fi

        oneapi_root="${ONEAPI_ROOT:-/opt/intel/oneapi}"
        MKL_ROOT="${MKL_ROOT:-${oneapi_root}/mkl/latest}"
        OMP_LIB_PATH="${OMP_LIB_PATH:-${oneapi_root}/compiler/latest/lib}"
        INTEL_LIB_PATHS="${MKL_ROOT}/lib/intel64:${OMP_LIB_PATH}"
        export MKLROOT="${MKL_ROOT}"
        export LIBRARY_PATH=${INTEL_LIB_PATHS}:$LIBRARY_PATH
        export LD_LIBRARY_PATH=${INTEL_LIB_PATHS}:$LD_LIBRARY_PATH

        cd /env/app
        DISKANN_ARCHIVE_URL=${DISKANN_ARCHIVE_URL:-"https://github.com/microsoft/DiskANN/archive/refs/heads/cpp_main.tar.gz"}
        wget -q "${DISKANN_ARCHIVE_URL}" -O diskann.tar.gz
        DISKANN_SRC_DIR=$(tar tf diskann.tar.gz | awk -F/ 'NR==1{print $1}')
        tar xf diskann.tar.gz
        pushd "${DISKANN_SRC_DIR}"
        cmake -B build \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_INSTALL_PREFIX=/usr/local \
            -DCMAKE_INSTALL_LIBDIR=lib64 \
            -DDISKANN_BUILD_APPS=OFF \
            -DDISKANN_BUILD_TESTS=OFF \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DBOOST_ROOT="${DISKANN_BOOST_ROOT}" \
            -DOMP_PATH="${OMP_LIB_PATH}" \
            -DBoost_NO_SYSTEM_PATHS=ON \
            .
        make -C build diskann -j4 && make -C build install
        cp -r /env/app/DiskANN-cpp_main/include /usr/local/include/diskann
        popd
    fi
else
    echo "DiskANN is not supported on arm64, skipping DiskANN and Boost build."
fi

GO_VERSION=1.22.5
cd /env/app
if [ $ARCH = "x86_64" ]; then
    if [ ! -f "go$GO_VERSION.linux-amd64.tar.gz" ]; then
        wget -q https://go.dev/dl/go$GO_VERSION.linux-amd64.tar.gz
    fi
    tar xf go$GO_VERSION.linux-amd64.tar.gz
    rm -rf go$GO_VERSION.linux-amd64.tar.gz
elif [ $ARCH = "aarch64" ]; then
    if [ ! -f "go$GO_VERSION.linux-arm64.tar.gz" ]; then
        wget -q https://go.dev/dl/go$GO_VERSION.linux-arm64.tar.gz
    fi
    tar xf go$GO_VERSION.linux-arm64.tar.gz
    rm -rf go$GO_VERSION.linux-arm64.tar.gz
fi
cp -r /env/app/go /usr/local/bin/go
