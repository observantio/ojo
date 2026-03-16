FROM almalinux:8

RUN dnf install -y --allowerasing \
      ca-certificates \
      curl \
      gcc \
      gcc-c++ \
      make \
      git \
      bash \
      pkgconf-pkg-config \
      openssl-devel \
      compat-openssl10 \
      compat-openssl10-devel \
    && dnf clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
