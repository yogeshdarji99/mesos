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

#ifndef __SIMULATOR_CONTAINERIZER_HPP__
#define __SIMULATOR_CONTAINERIZER_HPP__

#include <list>
#include <vector>
#include <regex>

#include <pthread.h>
#include <stdlib.h> // For random.

#include <mesos/slave/isolator.hpp>
#include <mesos/executor.hpp>

#include <process/future.hpp>
#include <process/metrics/counter.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/io.hpp>
#include <process/metrics/metrics.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>

#include <stout/hashmap.hpp>
#include <stout/duration.hpp>

#include "mesos/mesos.hpp"

#include "slave/state.hpp"
#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/simulator/taskspec.hpp"
#include "slave/containerizer/simulator/executor.hpp"

#include <mutex>

#include <pthread.h>

namespace mesos {
namespace internal {
namespace slave {

// Forward declaration.
class SimulatorContainerizerProcess;

// Adapted from TestContainerizer in src/tests/containerizer
class SimulatorContainerizer : public slave::Containerizer
{
public:
  static Try<SimulatorContainerizer*> create(const Flags& flags);

  SimulatorContainerizer(const std::vector <TaskSpec>& taskSpecs);

  virtual ~SimulatorContainerizer();

  virtual process::Future <Nothing> recover(
      const Option <state::SlaveState>& state);

  virtual process::Future <bool> launch(const ContainerID& containerId,
                                        const ExecutorInfo& executorInfo,
                                        const std::string& directory,
                                        const Option <std::string>& user,
                                        const SlaveID& slaveId,
                                        const process::PID <Slave>& slavePid,
                                        bool checkpoint);

  virtual process::Future <bool> launch(const ContainerID& containerId,
                                        const TaskInfo& taskInfo,
                                        const ExecutorInfo& executorInfo,
                                        const std::string& directory,
                                        const Option <std::string>& user,
                                        const SlaveID& slaveId,
                                        const process::PID <Slave>& slavePid,
                                        bool checkpoint);

  virtual process::Future <Nothing> update(const ContainerID& containerId,
                                           const Resources& resources);

  virtual process::Future <ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future <containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

  virtual process::Future <hashset<ContainerID>> containers();

private:
  process::Owned <SimulatorContainerizerProcess> process;
};


class SimulatorContainerizerProcess
    : public process::Process<SimulatorContainerizerProcess>
{
public:
  SimulatorContainerizerProcess(const std::vector <TaskSpec>& taskSpecs);

  virtual ~SimulatorContainerizerProcess();

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future <bool> launch(const ContainerID& containerId,
                                        const ExecutorInfo& executorInfo,
                                        const std::string& directory,
                                        const Option<std::string>& user,
                                        const SlaveID& slaveId,
                                        const process::PID<Slave>& slavePid,
                                        bool checkpoint);

  virtual process::Future <bool> launch(const ContainerID& containerId,
                                        const TaskInfo& taskInfo,
                                        const ExecutorInfo& executorInfo,
                                        const std::string& directory,
                                        const Option<std::string>& user,
                                        const SlaveID& slaveId,
                                        const process::PID<Slave>& slavePid,
                                        bool checkpoint);

  virtual process::Future <Nothing> update(const ContainerID& containerId,
                                           const Resources& resources);

  virtual process::Future <ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future <containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void delayDestroy(const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

  virtual process::Future<hashset<ContainerID>> containers();

private:
  // Delay between sending the terminal status update and setting the
  // termination promise
  static constexpr int TERMINATION_DELAY = 42;

  virtual bool _launch(const ContainerID& containerId,
                       const Option<TaskInfo>& taskInfo,
                       const ExecutorInfo& executorInfo,
                       const std::string& directory,
                       const Option<std::string>& user,
                       const SlaveID& slaveId,
                       const process::PID<Slave>& slavePid,
                       bool checkpoint);

  // Used to avoid race to environment variables.
  static std::mutex envLock;

  // Container struct. Encapsulates the executor, driver and promise.
  struct Container {
    // The termination promise.
    process::Promise< containerizer::Termination> promise;
    // The simulator executor.
    process::Owned<SimulatorExecutor> executor;
    // The executor driver.
    process::Owned<MesosExecutorDriver> driver;
  };

  // Hashmap of containerID to containers.
  hashmap<ContainerID, process::Owned<Container>> containers_;
  // The task specifications.
  const std::vector<TaskSpec> taskSpecs;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __SIMULATOR_CONTAINERIZER_HPP__
