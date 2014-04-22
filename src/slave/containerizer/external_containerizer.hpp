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

#ifndef __EXTERNAL_CONTAINERIZER_HPP__
#define __EXTERNAL_CONTAINERIZER_HPP__

#include <list>
#include <sstream>
#include <string>

#include <process/owned.hpp>
#include <process/subprocess.hpp>

#include <stout/hashmap.hpp>
#include <stout/protobuf.hpp>
#include <stout/try.hpp>
#include <stout/tuple.hpp>

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/isolator.hpp"
#include "slave/containerizer/launcher.hpp"

namespace mesos {
namespace internal {
namespace slave {

// The scheme an external containerizer has to adhere to is;
//
// COMMAND < INPUT-PROTO > RESULT-PROTO
//
// launch < containerizer::Launch
// update < containerizer::Update
// usage < mesos::ContainerID > mesos::ResourceStatistics
// wait < mesos::ContainerID > containerizer::Termination
// destroy < mesos::ContainerID
//
// 'wait' on the external containerizer side is expected to block
// until the task command/executor has terminated.
//

// Check src/examples/python/test_containerizer.py for a rough
// implementation template of this protocol.

// TODO(tillt): Implement a protocol for external containerizer
// recovery by defining needed protobuf/s.
// Currently we expect to cover recovery entirely on the slave side.

// For debugging purposes of an external containerizer, it might be
// helpful to enable verbose logging on the slave (GLOG_v=2).

class ExternalContainerizerProcess;

class ExternalContainerizer : public Containerizer
{
public:
  ExternalContainerizer(const Flags& flags);

  virtual ~ExternalContainerizer();

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future<Nothing> launch(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<slave::Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> launch(
      const ContainerID& containerId,
      const TaskInfo& task,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<slave::Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

private:
  ExternalContainerizerProcess* process;
};


class ExternalContainerizerProcess
  : public process::Process<ExternalContainerizerProcess>
{
public:
  ExternalContainerizerProcess(const Flags& flags);

  // Recover containerized executors as specified by state. See
  // containerizer.hpp:recover for more.
  process::Future<Nothing> recover(const Option<state::SlaveState>& state);

  // Start the containerized task.
  process::Future<Nothing> launch(
      const ContainerID& containerId,
      const TaskInfo& task,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  // Start the containerized executor.
  process::Future<Nothing> launch(
      const ContainerID& containerId,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  // Update the container's resources.
  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  // Gather resource usage statistics for the containerized executor.
  process::Future<ResourceStatistics> usage(const ContainerID& containerId);

  // Get a future on the containerized executor's Termination.
  process::Future<containerizer::Termination> wait(
      const ContainerID& containerId);

  // Terminate the containerized executor.
  void destroy(const ContainerID& containerId);

private:
  // Startup flags.
  const Flags flags;

  // Information describing a container environment. A sandbox has to
  // be prepared before the external containerizer can be invoked.
  struct Sandbox
  {
    Sandbox(const std::string& directory, const Option<std::string>& user)
      : directory(directory), user(user) {}

    const std::string directory;
    const Option<std::string> user;
  };

  // Information describing a running container.
  struct Container
  {
    Container(const Sandbox& sandbox) : sandbox(sandbox), pid(None()) {}

    // Keep sandbox information available for subsequent containerizer
    // invocations.
    Sandbox sandbox;

    // External containerizer pid as per wait-invocation.
    // Wait should block on the external containerizer side, hence we
    // need to keep its pid for terminating if needed.
    Option<pid_t> pid;

    process::Promise<containerizer::Termination> termination;

    // Used for delaying invocations of 'wait' until the external
    // containerizer has been fully launched.
    process::Promise<Nothing> launched;

    Resources resources;
  };

  // Stores all launched processes.
  hashmap<ContainerID, process::Owned<Container> > containers;

  process::Future<Nothing> _launch(
      const ContainerID& containerId,
      const SlaveID& slaveId,
      bool checkpoint,
      const process::Future<Option<int> >& future);

  process::Future<containerizer::Termination> _wait(
      const ContainerID& containerId);

  void __wait(
      const ContainerID& containerId,
      const process::Future<tuples::tuple<
          process::Future<Result<containerizer::Termination> >,
          process::Future<Option<int> > > >& future);

  process::Future<Nothing> _update(
      const ContainerID& containerId,
      const process::Future<Option<int> >& future);

  process::Future<ResourceStatistics> _usage(
      const ContainerID& containerId,
      const process::Future<tuples::tuple<
          process::Future<Result<ResourceStatistics> >,
          process::Future<Option<int> > > >& future);

  void _destroy(
      const ContainerID& containerId,
      const process::Future<Option<int> >& future);

  // Validate the invocation result.
  Try<Nothing> isDone(
      const process::Future<Option<int> >& future);

  // Validate the invocation results and extract a piped protobuf
  // message.
  template<typename T>
  Try<T> result(
      const process::Future<tuples::tuple<
          process::Future<Result<T> >,
          process::Future<Option<int> > > >& future)
  {
    if (!future.isReady()) {
      return Error("Could not receive any result");
    }

    Try<Nothing> status = isDone(tuples::get<1>(future.get()));
    if (status.isError()) {
      return Error(status.error());
    }

    process::Future<Result<T> > result = tuples::get<0>(future.get());
    if (result.isFailed()) {
      return Error("Could not receive any result (error: "
        + result.failure() + ")");
    }

    if (result.get().isError()) {
      return Error("Could not receive any result (error: "
        + result.get().error() + ")");
    }

    if (result.get().isNone()) {
      return Error("Could not receive any result");
    }

    return result.get().get();
  }

  // Terminate a containerizer process and its children.
  void terminate(const ContainerID& containerId);

  // Call back for when the containerizer has terminated all processes
  // in the container.
  void cleanup(const ContainerID& containerId);

  Try<process::Subprocess> invoke(
      const std::string& command,
      const Sandbox& sandbox,
      const google::protobuf::Message& message,
      const std::map<std::string, std::string>& environment
        = std::map<std::string, std::string>());
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __EXTERNAL_CONTAINERIZER_HPP__
