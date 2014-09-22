#ifndef EVENT_MANAGER_HPP
#define EVENT_MANAGER_HPP

#include <process/process.hpp>

#include "event_manager_base.hpp"
#include "http_proxy.hpp"

namespace process {

class EventManager : public internal::EventManager
{
public:
  virtual ~EventManager() {}

  virtual Socket accepted(int s) = 0;

  virtual void link(ProcessBase* process, const UPID& to) = 0;

  virtual PID<HttpProxy> proxy(const Socket& socket) = 0;

  virtual void send(Message* message) = 0;

  virtual Encoder* next(int s) = 0;

  virtual void close(int s) = 0;

  virtual void exited(const Node& node) = 0;
  virtual void exited(ProcessBase* process) = 0;

protected:
  EventManager() {}

};

} // namespace process {

#endif // EVENT_MANAGER_HPP
