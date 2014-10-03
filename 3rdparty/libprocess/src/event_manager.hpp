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

  /* A dependency injection of ProcessManager. This exposes just the
   * behavior that the EventManager cares about:
   *   - The ability to talk about a Process.
   *   - The ability to dispatch a request to be run. */
  class ProcessManager
  {
  protected:
    ProcessManager() {}

  public:
    virtual ~ProcessManager() {}

    /* Return a reference counted handle to the given process. */
    virtual ProcessReference use(const UPID& pid) = 0;

    /* Handle the given request for this socket. */
    virtual bool handle(
        const ConnectionHandle& connection_handle,
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

  /* A hook for initializing required state to run this manager. For
   * example initializing event loops. */
  virtual void initialize() = 0;

  /* Functions from original SocketManager. These are used by
   * ProcessManager to implement delivery of messages. */

  /* Establish a persistent connection between the given process and
   * the process represented by the UPID to. This is one way
   * connection from process -> to. See process::link() for more
   * details. */
  virtual void link(ProcessBase* process, const UPID& to) = 0;

  /* Return a handle to the HttpProxy representing the connection on
   * the given connection handle. */
  virtual PID<HttpProxy> proxy(const ConnectionHandle& connection_handle) = 0;

  /* Send the given message to the remote host identified in the
   * message. */
  virtual void send(Message* message) = 0;

  /* Created exited events for linked processes. See usage in
   * ProcessManager */
  virtual void exited(ProcessBase* process) = 0;

  /* Functions related to timer behavior. This behavior is usually
   * associated with io event managers as they can block indefinitely
   * for IO, and timers are used to set time-outs on waiting. */

  // Return the current time.
  virtual double get_time() const = 0;

  // Return true if there are pending timers that need to be executed.
  virtual bool has_pending_timers() const = 0;

  // Update the timer on async interrupt.
  virtual void update_timer() = 0;

  /* Update the timer on async interrupt iff it is not already set to
   * do so. */
  virtual void try_update_timer() = 0;

  /* Functions coming from process/io.hpp. The following are pure
   * virtuals that provide a nice hook for implementing each of
   * these functions in a clean way. */

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
