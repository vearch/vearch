FROM quay.io/centos/centos:stream9-minimal

ADD compile/bin /vearch/bin/
ADD compile/lib /vearch/lib/

COPY start.sh /vearch/start.sh
COPY restart.sh /vearch/restart.sh

RUN chmod +x /vearch/start.sh && chmod +x /vearch/restart.sh

RUN microdnf install -y dnf
RUN dnf --enablerepo=crb install -y libgomp lapack libzstd openblas-devel tbb cronie procps

ENTRYPOINT ["/vearch/start.sh"]
