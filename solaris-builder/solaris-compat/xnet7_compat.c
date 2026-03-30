#include <sys/socket.h>
#include <unistd.h>

long __sysconf_xpg7(int name) { return sysconf(name); }

int __xnet7_socket(int domain, int type, int protocol) { return socket(domain, type, protocol); }
int __xnet7_bind(int fd, const struct sockaddr *addr, socklen_t len) { return bind(fd, addr, len); }
int __xnet7_connect(int fd, const struct sockaddr *addr, socklen_t len) { return connect(fd, addr, len); }
int __xnet7_listen(int fd, int backlog) { return listen(fd, backlog); }
int __xnet7_getsockopt(int fd, int level, int optname, void *optval, socklen_t *optlen) {
  return getsockopt(fd, level, optname, optval, optlen);
}
int __xnet7_setsockopt(int fd, int level, int optname, const void *optval, socklen_t optlen) {
  return setsockopt(fd, level, optname, optval, optlen);
}
ssize_t __xnet7_sendto(int fd, const void *buf, size_t len, int flags, const struct sockaddr *dest, socklen_t dlen) {
  return sendto(fd, buf, len, flags, dest, dlen);
}
ssize_t __xnet7_sendmsg(int fd, const struct msghdr *msg, int flags) { return sendmsg(fd, msg, flags); }
ssize_t __xnet7_recvmsg(int fd, struct msghdr *msg, int flags) { return recvmsg(fd, msg, flags); }
int __xnet7_socketpair(int domain, int type, int protocol, int sv[2]) { return socketpair(domain, type, protocol, sv); }
