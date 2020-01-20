#!/bin/bash

path=`pwd`
rm -rf $path/fbs-gen
mkdir $path/fbs-gen

THIRD_PARTY=$path/../third_party

FLATBUFFERS=$THIRD_PARTY/$1

$FLATBUFFERS/flatc -g -o $2/go $path/fbs/gamma_api.fbs
$FLATBUFFERS/flatc -c -o $2/c $path/fbs/gamma_api.fbs
