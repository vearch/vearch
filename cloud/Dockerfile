FROM vearch/vearch-dev-env:latest AS builder

ENV GOROOT=/env/app/go
ENV PATH=$PATH:$GOROOT/bin
ENV ROCKSDB_HOME=/env/app/rocksdb_install
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ROCKSDB_HOME/lib

WORKDIR /vearch
COPY . .

RUN mkdir -p /env/app/go/src/github.com/vearch && \
    ln -s /vearch /env/app/go/src/github.com/vearch && \
    git config --global --add safe.directory /vearch && \
    cd /env/app/go/src/github.com/vearch/vearch/build && \
    ./build.sh && \
    mkdir -p /vearch/build/lib/ && \
    cp /env/app/rocksdb_install/lib/librocksdb.* /vearch/build/lib/ && \
    cp -r -p /usr/local/lib/libprotobuf.so* /vearch/build/lib/ && \
    cp /vearch/build/gamma_build/libgamma.* /vearch/build/lib/ && \
    rm -rf /vearch/build/gamma_build

FROM quay.io/centos/centos:stream9-minimal

COPY --from=builder /vearch/build/bin /vearch/bin/
COPY --from=builder /vearch/build/lib /vearch/lib/

COPY cloud/start.sh /vearch/start.sh
COPY cloud/restart.sh /vearch/restart.sh

RUN chmod +x /vearch/start.sh && chmod +x /vearch/restart.sh

RUN microdnf install -y dnf && dnf --enablerepo=crb install -y libgomp lapack libzstd openblas-devel tbb cronie procps

ENTRYPOINT ["/vearch/start.sh"]
