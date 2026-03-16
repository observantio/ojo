FROM centos:8

# CentOS 8 is EOL, so use vault repositories instead of mirrorlist.
RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && dnf clean all

RUN dnf install -y --allowerasing \
      ca-certificates curl gcc gcc-c++ make openssl-devel pkgconf-pkg-config git bash \
    && dnf clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
