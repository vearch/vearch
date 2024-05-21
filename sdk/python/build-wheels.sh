#!/bin/bash

ROOT_PATH=`pwd`/../..
OS=`uname -s`

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
    PY_TAGS=(3.7 3.8 3.9 3.10 3.11 3.12)
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
    for PYBIN in /opt/python/*/bin; do
        "${PYBIN}/pip" install -r dev-requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
        "${PYBIN}/python" setup.py bdist_wheel
        rm -rf build pyvearch.egg-info 
    done 
elif [ `expr substr ${OS} 1 10` == "MINGW" ];then
    echo "windows not support"
fi
