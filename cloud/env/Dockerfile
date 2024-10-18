FROM quay.io/centos/centos:stream9-minimal AS builder

WORKDIR /env
COPY env.sh .

RUN microdnf install -y dnf && \
    dnf install -y epel-release && \
    dnf install -y wget make automake git which libzstd-devel openssl-devel tbb-devel boost-devel tar findutils gcc-c++ && \
    cd /env && ./env.sh

FROM quay.io/centos/centos:stream9-minimal

COPY --from=builder /env/app/rocksdb_install /env/app/rocksdb_install
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /usr/local/lib/libprotobuf* /usr/local/lib/
COPY --from=builder /usr/local/include/google /usr/local/include/google

ENV ROCKSDB_HOME=/env/app/rocksdb_install
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROCKSDB_HOME/lib
