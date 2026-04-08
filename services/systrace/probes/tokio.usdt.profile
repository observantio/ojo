name: tokio
kind: usdt
targets:
  - binary: /usr/bin/*
    provider: tokio
    probes: [task_spawn, task_poll_start, task_poll_end, task_wake]
