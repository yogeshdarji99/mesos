#ifndef LIBEV_EVENT_MANAGER_HPP
#define LIBEV_EVENT_MANAGER_HPP

#include "event_manager.hpp"

namespace process {

// singelton for PIMPL pattern libev based event manager
  extern EventManager* GetLibevEventManager(
      EventManager::ProcessManager* process_manager);

} // namespace process {

#endif // LIBEV_EVENT_MANAGER_HPP
