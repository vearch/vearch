FROM centos:7

ADD compile/bin /vearch/bin/
ADD compile/lib /vearch/lib/

ENV MASTER_NAME="___MASTER_NAME___"
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/vearch/lib/"

RUN yum install -y libgomp blas-devel lapack-devel

ENTRYPOINT ["/vearch/bin/vearch","-conf" ,"/vearch/config.toml","-master","$MASTER_NAME"]