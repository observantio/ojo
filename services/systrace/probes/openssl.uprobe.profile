name: openssl
kind: uprobe
targets:
  - library: libssl.so
    symbols: [SSL_read, SSL_write, SSL_do_handshake]
