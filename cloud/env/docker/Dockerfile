FROM centos:7
RUN yum -y update
RUN yum install -y epel-release wget gcc gcc-c++ make automake git blas-devel lapack-devel which openssl-devel libzstd-devel openblas-devel tbb-devel &&\
    mkdir -p /env/app &&\
    cd /env/app/ &&\
    wget https://cmake.org/files/v3.20/cmake-3.20.0-rc3.tar.gz &&\
    tar xf cmake-3.20.0-rc3.tar.gz &&\
    cd /env/app/cmake-3.20.0-rc3 &&\
    ./bootstrap &&\
    gmake &&\
    gmake install &&\
    cd /usr/bin &&\
    ln -s cmake3 cmake &&\
    cd /env/app &&\
    wget https://github.com/facebook/rocksdb/archive/v6.2.2.tar.gz -O rocksdb-v6.2.2.tar.gz &&\
    tar -xzf rocksdb-v6.2.2.tar.gz &&\
    cd /env/app/rocksdb-6.2.2 &&\
    make shared_lib &&\
    mkdir -p /env/app/rocksdb_install/lib &&\
    cp librocksdb.so.6.2.2 /env/app/rocksdb_install/lib &&\
    cd /env/app/rocksdb_install/lib &&\
    ln -s librocksdb.so.6.2.2 librocksdb.so.6.2 &&\
    ln -s librocksdb.so.6.2 librocksdb.so &&\
    cp -r /env/app/rocksdb-6.2.2/include /env/app/rocksdb_install/ &&\
    cd /env/app/ &&\
    wget https://dl.google.com/go/go1.12.7.linux-amd64.tar.gz &&\
    tar -xzf go1.12.7.linux-amd64.tar.gz
