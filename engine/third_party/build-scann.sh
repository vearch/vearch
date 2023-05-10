#!/bin/bash
ROOT=$(dirname "$PWD")

if [ ! -d "scann" ]; then
    SCANN_HOME=$ROOT/third_party/scann
    echo "begin build scann"
    cd scann-1.2.1
    python configure.py
    if [ $? != 0 ]; then
        echo "scann python configure.py error"
        exit 1
    fi 
    CC=clang bazel build -c opt --features=thin_lto --copt=-mavx2 --copt=-mfma --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0" --cxxopt="-std=c++17" --incompatible_linkopts_to_linklibs --copt=-fsized-deallocation --copt=-w :build_pip_pkg
    if [ $? != 0 ]; then
        echo "scann bazel build error"
        exit 2
    fi 
    cd ..
    mkdir -p $SCANN_HOME/lib
    mv scann-1.2.1/bazel-bin/scann/scann_ops/cc/libscannapi.so $SCANN_HOME/lib
    echo "build scann success!!!"
fi

