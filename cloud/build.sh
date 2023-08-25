#!/usr/bin/env bash
VERSION=latest
ROOT=`dirname ${PWD}`

function get_version() {
  VEARCH_VERSION_MAJOR=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_MAJOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_MINOR=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_MINOR | awk -F' ' '{print $2}'`
  VEARCH_VERSION_PATCH=`cat ${ROOT}/VERSION | grep VEARCH_VERSION_PATCH | awk -F' ' '{print $2}'`

  VERSION="v${VEARCH_VERSION_MAJOR}.${VEARCH_VERSION_MINOR}.${VEARCH_VERSION_PATCH}"
  echo "VERSION="${VERSION}
}

cp -r ../build/bin compile/
cp -r ../build/lib compile/
docker build -t vearch/vearch:$VERSION .
rm -rf compile/bin
rm -rf compile/lib
