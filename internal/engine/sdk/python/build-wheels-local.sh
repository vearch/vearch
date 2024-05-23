#!/bin/bash

ROOT_PATH=`pwd`/../../../..
BASE_PATH=$ROOT_PATH"/internal/engine"
OS=`uname -s`
cp -r $BASE_PATH/idl/fbs-gen/python/* ./python

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

if [ `expr substr ${OS} 1 5` == "Linux" ];then
    #Compile wheels
    export GAMMA_LDFLAGS=$BASE_PATH/build/libgamma.so
    export GAMMA_INCLUDE=$BASE_PATH
    export LD_LIBRARY_PATH=$BASE_PATH/build/:$LD_LIBRARY_PATH

    pip install -r dev-requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
    python setup.py bdist_wheel
    auditwheel repair dist/vearch* 
    rm -rf dist build vearch.egg-info 
    pip install wheelhouse/vearch* --force-reinstall
    python -c "import vearch"
fi
