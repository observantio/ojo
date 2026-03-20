FROM centos:8 AS rust-builder

# CentOS 8 is EOL, so use vault repositories instead of mirrorlist.
RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && dnf clean all

RUN dnf install -y --allowerasing --setopt=install_weak_deps=False \
      ca-certificates \
      curl \
      bash \
    && dnf clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

FROM centos:8

# CentOS 8 is EOL, so use vault repositories instead of mirrorlist.
RUN sed -i \
      -e 's|^mirrorlist=|#mirrorlist=|g' \
      -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' \
      /etc/yum.repos.d/CentOS-* \
 && dnf clean all

RUN dnf install -y --allowerasing --setopt=install_weak_deps=False \
      ca-certificates \
      gcc \
      gcc-c++ \
      make \
      openssl-devel \
      pkgconf-pkg-config \
      git \
      bash \
    && dnf clean all

COPY --from=rust-builder /root/.cargo /root/.cargo
COPY --from=rust-builder /root/.rustup /root/.rustup

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
