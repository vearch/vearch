FROM quay.io/centos/centos:stream9-minimal

RUN microdnf install -y dnf
RUN dnf install -y epel-release
RUN dnf install -y wget make automake git which libzstd-devel openssl-devel tbb-devel boost-devel tar findutils gcc-c++
RUN dnf --enablerepo=crb install -y blas-devel lapack-devel openblas-devel
COPY env.sh /env/env.sh

RUN /env/env.sh
