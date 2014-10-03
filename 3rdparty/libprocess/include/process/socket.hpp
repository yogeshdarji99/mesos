#ifndef __PROCESS_SOCKET_HPP__
#define __PROCESS_SOCKET_HPP__

#include <assert.h>
#include <unistd.h> // For close.

#include <iostream>

#include <stout/nothing.hpp>
#include <stout/os.hpp>
#include <stout/try.hpp>


namespace process {

// Returns a socket fd for the specified options. Note that on OS X,
// the returned socket will have the SO_NOSIGPIPE option set.
inline Try<int> socket(int family, int type, int protocol) {
  int s;
  if ((s = ::socket(family, type, protocol)) == -1) {
    return ErrnoError();
  }

#ifdef __APPLE__
  // Disable SIGPIPE via setsockopt because OS X does not support
  // the MSG_NOSIGNAL flag on send(2).
  const int enable = 1;
  if (setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enable, sizeof(int)) == -1) {
    return ErrnoError();
  }
#endif // __APPLE__

  return s;
}


class Connection {
public:
  typedef int id_t;
  virtual ~Connection() {}

  inline operator const id_t&() const
  {
    return id;
  }

protected:
  Connection(const id_t& _id) : id(_id) {}

  /* This is good enough for now. In the future we might not want to
   * limit ourselves to representing the identifier as an int (fd). */
  const id_t id;
};


typedef std::shared_ptr<Connection> ConnectionHandle;


} // namespace process {

#endif // __PROCESS_SOCKET_HPP__
