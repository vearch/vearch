#!/usr/bin/env bash
cd pspb

echo $0 "[out_dir [source_dir]]"

#check_protoc_version

PROGRAM=$(basename "$0")

# check protoc exist
command -v protoc >/dev/null 2>&1 || { echo >&2 "ERR: protoc is required but it's not installed.  Aborting."; exit 1; }

# find protoc-gen-gofast
GOGO_GOPATH=""
for path in $(echo "${GOPATH}" | sed -e 's/:/ /g'); do
    gogo_proto_bin="${path}/bin/protoc-gen-gofast"
    if [ -e "${gogo_proto_bin}" ]; then
        export PATH=$(dirname "${gogo_proto_bin}"):$PATH
        GOGO_GOPATH=${path}
        break
    fi
done

# protoc-gen-gofast not found
if [[ -z ${GOGO_GOPATH} ]]; then
    echo >&2 "ERR: Could not find protoc-gen-gofast"
    echo >&2 "Please run \`go get github.com/gogo/protobuf/protoc-gen-gofast\` first"
    exit 1;
fi





gen_out_dir=.
if [ "$1" ]; then
    gen_out_dir=$1
    mkdir -p $gen_out_dir
fi

proto_dir=.
if [ "$2" ]; then
    proto_dir=$2
fi

ret=0
for file in `ls ${proto_dir}/*.proto`
do
    protoc -I${proto_dir}:${GOGO_GOPATH}/src:${GOGO_GOPATH}/src/github.com/gogo/protobuf --gofast_out=plugins=grpc,$GO_OUT_M:$gen_out_dir $file || ret=$?
    pb_files=${gen_out_dir}/*.pb.go
    rm -f ${gen_out_dir}/*.bak
    goimports -w $pb_files
done
exit $ret
