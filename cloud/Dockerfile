FROM vearch/vearch-dev-env:latest AS builder

WORKDIR /vearch
COPY . .

RUN dnf install -y epel-release && \
    dnf install -y wget make cmake automake which libzstd-devel openssl-devel tbb-devel tar findutils gcc-c++ && \
    dnf --enablerepo=crb install -y blas-devel lapack-devel openblas-devel golang libuv && \
    mkdir -p /env/app/go/src/github.com/vearch && \
    ln -s /vearch /env/app/go/src/github.com/vearch && \
    git config --global --add safe.directory /vearch && \
    cd /env/app/go/src/github.com/vearch/vearch/build && \
    ./build.sh && \
    mkdir -p /vearch/build/lib/ && \
    cp -r -p /usr/local/lib/libprotobuf.so* /vearch/build/lib/ && \
    cp /vearch/build/gamma_build/libgamma*.so /vearch/build/lib/ && \
    rm -rf /vearch/build/gamma_build

FROM quay.io/centos/centos:stream9

COPY --from=builder /vearch/build/bin /vearch/bin/
COPY --from=builder /vearch/build/lib /vearch/lib/
COPY --from=builder /lib64/libopenblas.so /lib64/

RUN if [ "$(uname -m)" = "x86_64" ]; then \
        echo "Setting gamma for x86_64"; \
        rm -rf /vearch/lib/libgamma.so && \
        ln -s /vearch/lib/libgamma_avx2.so /vearch/lib/libgamma.so; \
    fi

ENV LD_LIBRARY_PATH=/vearch/lib/:$LD_LIBRARY_PATH
RUN dnf --enablerepo=crb install -y libgomp lapack libzstd tbb cronie procps

ENTRYPOINT ["/vearch/bin/vearch", "-conf", "/vearch/config.toml"]
CMD ["all"]
