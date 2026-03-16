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
    glibc-headers \
    glibc-devel \
    libstdc++ \
    libstdc++-devel \
    && dnf clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | bash -s -- -y --profile minimal

ENV PATH="/root/.cargo/bin:${PATH}"

ENV RUSTFLAGS="-C target-cpu=x86-64 -C target-feature=-sse4.2,-avx,-avx2"

WORKDIR /workspace
