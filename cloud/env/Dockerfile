FROM quay.io/centos/centos:stream9 AS builder

WORKDIR /env
COPY install-dependencies.sh .

RUN dnf install -y epel-release && \
    dnf install -y wget make automake cmake git which libzstd-devel openssl-devel tbb-devel boost-devel tar findutils gcc-c++ && \
    dnf --enablerepo=crb install -y blas-devel lapack-devel openblas-devel && \
    cd /env && ./install-dependencies.sh

FROM quay.io/centos/centos:stream9

COPY --from=builder /usr/local/include/rocksdb /usr/local/include/rocksdb
COPY --from=builder /usr/local/lib/librocksdb.a /usr/local/lib/
COPY --from=builder /usr/local/bin/protoc /usr/local/bin/protoc
COPY --from=builder /usr/local/lib/libprotobuf* /usr/local/lib/
COPY --from=builder /usr/local/include/google /usr/local/include/google
COPY --from=builder /usr/local/lib64/libroaring.a /usr/local/lib64/libroaring.a
COPY --from=builder /usr/local/include/roaring /usr/local/include/roaring
COPY --from=builder /usr/local/lib64/libfaiss* /usr/local/lib64/
COPY --from=builder /usr/local/include/faiss /usr/local/include/faiss
COPY --from=builder /usr/local/share/faiss /usr/local/share/faiss

RUN dnf install -y git
