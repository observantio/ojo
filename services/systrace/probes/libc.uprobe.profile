name: libc
kind: uprobe
targets:
  - library: libc.so.6
    symbols: [read, write, open, openat, close, connect, accept4, epoll_wait]
