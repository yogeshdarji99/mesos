#ifndef LIBEVENT_EVENT_MANAGER_HPP
#define LIBEVENT_EVENT_MANAGER_HPP

#include "event_manager.hpp"

namespace process {

// singelton for PIMPL pattern libevent based event manager
  extern EventManager* GetLibeventEventManager(
      EventManager::ProcessManager* process_manager);

} // namespace process {

#endif // LIBEVENT_EVENT_MANAGER_HPP
