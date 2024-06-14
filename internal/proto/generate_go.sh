#!/usr/bin/env bash

echo $0 "[out_dir [source_dir]]"

PROGRAM=$(basename "$0")

# check protoc exist
command -v protoc >/dev/null 2>&1 || {
    echo >&2 "ERR: protoc is required but it's not installed.  Aborting."
    exit 1
}

#check_protoc_version
function version_gt() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" != "$1"; }
function version_le() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" == "$1"; }
function version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }
function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

cmd_exists() {
    #which "$1" 1>/dev/null 2>&1
    which "$1"
}

check_protoc_version() {
    version=$(protoc --version | awk -F"[ ]" '{print $2}')
    echo "protoc current version is "$version
    if version_lt $version "3.1.0"; then
        echo "Error : version not match, version 3.1.0 or above is needed"
        exit 1
    fi
}

go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
check_protoc_version

gen_out_dir=./vearchpb
if [ "$1" ]; then
    gen_out_dir=$1
    mkdir -p $gen_out_dir
fi

proto_dir=.
if [ "$2" ]; then
    proto_dir=$2
fi

export PATH=$PATH:$GOPATH/bin
protoc --go_out=$proto_dir *.proto
exit $?
