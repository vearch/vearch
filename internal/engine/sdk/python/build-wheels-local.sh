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

get_version() {
  VEARCH_VERSION_MAJOR=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_MINOR=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_PATCH=`cat ${ROOT_PATH}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}'`

  VEARCH_VERSION="${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  export VERSION=${VEARCH_VERSION}
  echo "VERSION="${VERSION}
}

get_version

if [ "$(uname -s | cut -c1-5)" = "Linux" ]; then
    #Compile wheels
    export GAMMA_LDFLAGS=$GAMMA_LIBRARY_PATH/libgamma_avx2.so
    export GAMMA_INCLUDE=$BASE_PATH
    export LD_LIBRARY_PATH=$GAMMA_LIBRARY_PATH:$LD_LIBRARY_PATH

    pip install -r dev-requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
    python setup.py bdist_wheel
    # auditwheel repair dist/vearch* 
    # rm -rf dist build vearch.egg-info 
    pip install dist/vearch* --force-reinstall
    python -c "import vearch"
fi
