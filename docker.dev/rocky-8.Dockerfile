FROM rockylinux:8 AS rust-builder

RUN yum install -y \
      ca-certificates \
      curl \
      bash \
    && yum clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

FROM rockylinux:8

RUN yum install -y \
      ca-certificates \
      gcc \
      gcc-c++ \
      make \
      openssl-devel \
      pkgconf-pkg-config \
      git \
      bash \
    && yum clean all

COPY --from=rust-builder /root/.cargo /root/.cargo
COPY --from=rust-builder /root/.rustup /root/.rustup

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
