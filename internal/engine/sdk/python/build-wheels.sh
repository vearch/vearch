#!/bin/bash

ROOT_PATH=`pwd`/../../../..
BASE_PATH=$ROOT_PATH"/internal/engine"
OS=`uname -s`
cp -r $BASE_PATH/idl/fbs-gen/python/* ./python

GAMMA_LIBRARY_PATH=""

# Check if ROOT_PATH/build/gamma_build exists
if [ -d "$ROOT_PATH/build/gamma_build" ]; then
    GAMMA_LIBRARY_PATH="$ROOT_PATH/build/gamma_build"
    echo "Gamma library path set to: $GAMMA_LIBRARY_PATH"
else
    GAMMA_LIBRARY_PATH=$BASE_PATH/build/
    echo "Gamma library path set to: $GAMMA_LIBRARY_PATH"
fi

VERSION="lastest"

function get_version() {
  VEARCH_VERSION_MAJOR=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_MINOR=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_PATCH=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}'`

  VEARCH_VERSION="${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  export VERSION=${VEARCH_VERSION}
  echo "VERSION="${VERSION}
}

get_version

if [ ${OS} == "Darwin" ];then
    export GAMMA_LDFLAGS=$GAMMA_LIBRARY_PATH/libgamma_avx2.dylib
    PY_TAGS=(3.6 3.7 3.8 3.9)
    for TAG in ${PY_TAGS[*]} 
    do
        PY_NAME=python${TAG}
        conda create -n ${PY_NAME} python=${TAG}  --y
        source activate
        conda activate ${PY_NAME}
        pip install -r dev-requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
        python setup.py bdist_wheel
    done
elif [ `expr substr ${OS} 1 5` == "Linux" ];then
    #Compile wheels
    export GAMMA_LDFLAGS=$GAMMA_LIBRARY_PATH/libgamma_avx2.so
    export GAMMA_INCLUDE=$BASE_PATH
    export LD_LIBRARY_PATH=$GAMMA_LIBRARY_PATH:$LD_LIBRARY_PATH
    for PYBIN in /opt/python/*/bin; do
        "${PYBIN}/pip" install -r dev-requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
        "${PYBIN}/python" setup.py bdist_wheel
        auditwheel repair dist/vearch* 
        rm -rf dist build vearch.egg-info 
    done 
elif [ `expr substr ${OS} 1 10` == "MINGW" ];then
    echo "windows not support"
fi
