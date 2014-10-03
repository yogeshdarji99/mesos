#ifndef EVENT_MANAGER_BASE_HPP
#define EVENT_MANAGER_BASE_HPP

#include "encoder.hpp"

namespace process {

namespace internal {

class EventManager
{
public:
  virtual ~EventManager() {}

  virtual void send(Encoder* encoder, bool persist) = 0;

  virtual void send(
      const http::Response& response,
      const http::Request& request,
      const ConnectionHandle& connection_handle) = 0;

protected:
  EventManager() {}

};

}  // namespace internal {

} // namespace process {

#endif // EVENT_MANAGER_BASE_HPP
