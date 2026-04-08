name: network
kind: ebpf
targets:
  - hook: kprobe/tcp_retransmit_skb
  - hook: tracepoint/net/net_dev_queue
  - hook: tracepoint/net/netif_receive_skb
