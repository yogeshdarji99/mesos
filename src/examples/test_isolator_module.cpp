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

#include <list>
#include <string>

#include <process/dispatch.hpp>
#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <stout/try.hpp>

#include <mesos/module.hpp>
#include <tests/module.hpp>

#include <slave/containerizer/isolator.hpp>

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

class TestIsolatorProcess : public IsolatorProcess
{
public:
  virtual ~TestIsolatorProcess() {}

  virtual process::Future<Nothing> recover(
      const std::list<state::RunState>& state) {
    return Nothing();
  }

  virtual process::Future<Option<CommandInfo> > prepare(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo)
  {
    return None();
  }

  virtual process::Future<Nothing> isolate(
      const ContainerID& containerId,
      pid_t pid)
  {
    return Nothing();
  }

  virtual process::Future<Limitation> watch(
      const ContainerID& containerId)
  {
    return Limitation(Resource(), "");
  }

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources)
  {
    return Nothing();
  }

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId)
  {
    return ResourceStatistics();
  }

  virtual process::Future<Nothing> cleanup(const ContainerID& containerId)
  {
    return Nothing();
  }
};

MESOS_MODULE(Isolator, testIsolator)
{
  process::Owned<IsolatorProcess> process(new TestIsolatorProcess);
  return new Isolator(process);
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {

