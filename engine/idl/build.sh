#!/bin/bash

BASE_PATH=`pwd`
FBS_GEN_PATH=fbs-gen

THIRD_PARTY=$BASE_PATH/../third_party
declare FLATBUFFERS_VERSION

if [ "$(uname)" == "Darwin" ]; then
  FLATBUFFERS_VERSION=2.0.0
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then   
  FLATBUFFERS_VERSION=1.11.0
fi

if [ ! -d "$THIRD_PARTY/flatbuffers-$FLATBUFFERS_VERSION" ]; then
  cd $THIRD_PARTY
  wget https://github.com/google/flatbuffers/archive/v${FLATBUFFERS_VERSION}.tar.gz
  tar xf v${FLATBUFFERS_VERSION}.tar.gz
  rm -rf v${FLATBUFFERS_VERSION}.tar.gz
  cd flatbuffers-${FLATBUFFERS_VERSION}
  cmake . && make -j2
  cd ..
  rm -rf flatbuffers
  cp -r -p flatbuffers-${FLATBUFFERS_VERSION}/include/flatbuffers .
fi

cd $BASE_PATH

rm -rf $FBS_GEN_PATH
mkdir $FBS_GEN_PATH

FLATBUFFERS=$THIRD_PARTY/flatbuffers-${FLATBUFFERS_VERSION}

$FLATBUFFERS/flatc -g -o $FBS_GEN_PATH/go $BASE_PATH/fbs/*.fbs --go-namespace gamma_api
$FLATBUFFERS/flatc -c --no-prefix -o $FBS_GEN_PATH/c $BASE_PATH/fbs/*.fbs
$FLATBUFFERS/flatc -p -o $FBS_GEN_PATH/python $BASE_PATH/fbs/*.fbs

