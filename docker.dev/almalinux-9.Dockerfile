FROM almalinux:9 AS rust-builder

RUN dnf install -y --allowerasing --setopt=install_weak_deps=False \
      ca-certificates \
      curl \
      bash \
    && dnf clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

FROM almalinux:9

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
