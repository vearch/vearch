FROM centos:7
RUN yum -y update
RUN yum install -y wget gcc gcc-c++ make automake git blas-devel lapack-devel which &&\
    mkdir -p /env/app &&\
    cd /env/app/ &&\
    wget https://cmake.org/files/v3.12/cmake-3.12.4.tar.gz &&\
    tar -xzf cmake-3.12.4.tar.gz &&\
    cd /env/app/cmake-3.12.4 &&\
    ./bootstrap &&\
    gmake &&\
    gmake install &&\
    cd /usr/bin &&\
    ln -s cmake3 cmake &&\
    cd /env/app/ &&\
    git clone https://github.com/facebookresearch/faiss.git &&\
    #cd faiss &&\
    #git checkout v1.6.0 &&\
    cd /env/app/faiss &&\
    ./configure --without-cuda --prefix=/env/app/faiss_install &&\
    make &&\
    make install &&\
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
