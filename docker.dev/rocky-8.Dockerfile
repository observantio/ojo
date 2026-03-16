FROM rockylinux:8

RUN yum install -y \
      ca-certificates curl gcc gcc-c++ make openssl-devel pkgconf-pkg-config git bash \
    && yum clean all

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
