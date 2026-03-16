FROM centos:7

RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && yum clean all

RUN yum install -y \
      epel-release \
      ca-certificates curl gcc gcc-c++ make openssl-devel pkgconfig git bash \
    && yum clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
