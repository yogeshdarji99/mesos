#ifndef EVENT_MANAGER_HPP
#define EVENT_MANAGER_HPP

#include "event_manager_base.hpp"

namespace process {

class EventManager : public internal::EventManager
{
public:
  virtual ~EventManager() {}

protected:
  EventManager() {}

};

} // namespace process {

#endif // EVENT_MANAGER_HPP
