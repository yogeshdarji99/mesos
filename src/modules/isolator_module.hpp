/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __ISOLATOR_MODULE_HPP__
#define __ISOLATOR_MODULE_HPP__

#include <stout/try.hpp>
#include <stout/memory.hpp>

#include <modules/module.hpp>

#include <slave/containerizer/isolator.hpp>

using namespace process;


namespace mesos {
namespace internal {
namespace slave {

class IsolatorModule : public Module, public Isolator {
public:
  IsolatorModule(process::Owned<IsolatorProcess> process)
    : Module(Module::ISOLATOR_MODULE),
      Isolator(process) {}

  static Try<memory::shared_ptr<Isolator> > init(DynamicLibrary& library)
  {
    Try<memory::shared_ptr<IsolatorModule> > module =
      module::init<IsolatorModule>(library, "create_isolator_module");

    if (module.isError()) {
      return Error(module.error());
    }

    return std::dynamic_pointer_cast<Isolator>(module.get());
  }
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __TESTS_MODULE_HPP__
