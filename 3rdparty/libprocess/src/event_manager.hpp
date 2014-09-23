#ifndef EVENT_MANAGER_HPP
#define EVENT_MANAGER_HPP

#include <process/process.hpp>

#include "event_manager_base.hpp"
#include "http_proxy.hpp"
#include "process_reference.hpp"
#include "synchronized.hpp"

namespace process {

class EventManager : public internal::EventManager
{
public:

  class ProcessManager
  {
  protected:
    ProcessManager() {}

  public:
    virtual ~ProcessManager() {}

    virtual ProcessReference use(const UPID& pid) = 0;

    virtual bool handle(
        const Socket& socket,
        http::Request* request) = 0;
  };

  virtual ~EventManager() {}

  /* Forward the enqueue call from a more derived class. This is a proxy call
   * as ProcessBase can not friend all derived versions of this class. They may
   * not be known at compile time. */
  inline void enqueue(ProcessBase* proc_base,
      Event* event,
      bool inject = false);

  /* Return the pid from the given process. This is a proxy call similar to the
   * above. */
  inline const UPID &get_pid(ProcessBase* process) const;

  // TODO(benh): ev_time() versus ev_now() in libev?
  virtual double get_time() const = 0;

  virtual void initialize() = 0;

  virtual Socket accepted(int s) = 0;

  virtual void link(ProcessBase* process, const UPID& to) = 0;

  virtual PID<HttpProxy> proxy(const Socket& socket) = 0;

  virtual void send(Message* message) = 0;

  virtual Encoder* next(int s) = 0;

  virtual void close(int s) = 0;

  virtual void exited(const Node& node) = 0;
  virtual void exited(ProcessBase* process) = 0;

  virtual bool has_pending_timers() const = 0;

  virtual void update_timer() = 0;

  virtual void try_update_timer() = 0;

  // see process/io.hpp
  virtual Future<short> poll(int fd, short events) = 0;

  // see process/io.hpp
  virtual Future<size_t> read(int fd, void* data, size_t size) = 0;

  // see process/io.hpp
  virtual Future<std::string> read(int fd) = 0;

  // see process/io.hpp
  virtual Future<size_t> write(int fd, void* data, size_t size) = 0;

  // see process/io.hpp
  virtual Future<Nothing> write(int fd, const std::string& data) = 0;

  // see process/io.hpp
  virtual Future<Nothing> redirect(int from, Option<int> to, size_t chunk) = 0;

protected:
  EventManager() {}

};

inline void EventManager::enqueue(
    ProcessBase* proc_base,
    Event* event,
    bool inject)
{
  proc_base->enqueue(event, inject);
}

inline const UPID &EventManager::get_pid(ProcessBase* process) const
{
  return process->pid;
}

extern std::map<Time, std::list<Timer> >* timeouts;
extern synchronizable(timeouts);

// Unique id that can be assigned to each process.
extern uint32_t __id__;

// Local server socket.
extern int __s__;

// Local IP address.
extern uint32_t __ip__;

// Local port.
extern uint16_t __port__;

} // namespace process {

#endif // EVENT_MANAGER_HPP
