# Systrace Runtime Probe Profiles

This directory contains optional probe profiles used by `ojo-systrace`.

Profiles:
- `tokio.usdt.profile`: user-space static tracing points expected for async runtimes.
- `libc.uprobe.profile`: key libc call boundaries for syscall and IO attribution.
- `openssl.uprobe.profile`: TLS and crypto boundaries for request reconstruction.
- `network.ebpf.profile`: kernel network hooks for flow and retransmit visibility.

These are declarative profile files consumed by deployment automation. The systrace service exports
`system.systrace.runtime.probes.configured.total` from configured profile names.
