#!/usr/bin/env bash

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" != "$1"; }
function version_le() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" == "$1"; }
function version_lt() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" != "$1"; }
function version_ge() { test "$(echo "$@" | tr " " "\n" | sort -rV | head -n 1)" == "$1"; }

cmd_exists () {
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

check_protoc_version

