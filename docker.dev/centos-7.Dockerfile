FROM centos:7 AS rust-builder

RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && yum clean all

RUN yum install -y \
      epel-release \
      ca-certificates \
      curl \
      bash \
    && yum clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

FROM centos:7

RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && yum clean all

RUN yum install -y \
      epel-release \
      ca-certificates \
      gcc \
      gcc-c++ \
      make \
      openssl-devel \
      pkgconfig \
      git \
      bash \
    && yum clean all

COPY --from=rust-builder /root/.cargo /root/.cargo
COPY --from=rust-builder /root/.rustup /root/.rustup

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
