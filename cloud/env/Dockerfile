FROM quay.io/centos/centos:stream9 AS builder

WORKDIR /env
COPY install-dependencies.sh .

RUN dnf install -y epel-release && \
    dnf install -y wget make automake cmake git which libzstd-devel openssl-devel tbb-devel boost-devel tar findutils gcc-c++ && \
    dnf --enablerepo=crb install -y blas-devel lapack-devel openblas-devel && \
    wget -q https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB -O /etc/pki/rpm-gpg/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB && \
    rpm --import /etc/pki/rpm-gpg/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB && \
    printf '[oneAPI]\nname=Intel\xc2\xae oneAPI repository\nbaseurl=https://yum.repos.intel.com/oneapi\nenabled=1\ngpgcheck=1\nrepo_gpgcheck=1\ngpgkey=https://yum.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB\n' > /etc/yum.repos.d/oneAPI.repo && \
    if [ "$(arch)" = "x86_64" ]; then dnf install -y libaio-devel gperftools intel-oneapi-mkl-devel-2025.1; fi && \
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

RUN --mount=type=bind,from=builder,src=/usr/local/lib64,dst=/tmp/lib \
    --mount=type=bind,from=builder,src=/usr/local/include,dst=/tmp/include \
    if [ "$(arch)" = "x86_64" ]; then \
        cp -n /tmp/lib/libdiskann* /usr/local/lib64/; \
        cp -r /tmp/include/diskann /usr/local/include/diskann/; \
    fi

COPY --from=builder /usr/local/bin/go /usr/local/bin/go

RUN dnf install -y git
