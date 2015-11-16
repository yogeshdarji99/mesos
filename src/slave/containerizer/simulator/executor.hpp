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

#ifndef __SIMULATOR_EXECUTOR_HPP__
#define __SIMULATOR_EXECUTOR_HPP__

#include <process/process.hpp>
#include <process/owned.hpp>
#include <process/future.hpp>

#include "mesos/executor.hpp"

#include "slave/containerizer/simulator/taskspec.hpp"

namespace mesos {
namespace internal {
namespace slave {

// Forward declaration.
class SimulatorExecutorProcess;

class SimulatorExecutor : public mesos::Executor
{
public:
  SimulatorExecutor(const std::vector<TaskSpec>& taskSpecs);

  virtual ~SimulatorExecutor();

  virtual void registered(ExecutorDriver* driver,
                          const ExecutorInfo& executorInfo,
                          const FrameworkInfo& frameworkInfo,
                          const SlaveInfo& slaveInfo) {}

  virtual void reregistered(ExecutorDriver* driver,
                            const SlaveInfo& slaveInfo) {}

  virtual void disconnected(ExecutorDriver* driver) {}

  virtual void frameworkMessage(ExecutorDriver* driver,
                                const std::string& data) {}

  virtual void error(ExecutorDriver* driver, const std::string& message) {}

  virtual void launchTask(ExecutorDriver* driver, const TaskInfo& task);

  virtual void killTask(ExecutorDriver* driver, const TaskID& taskId);

  virtual void shutdown(ExecutorDriver* driver);

  virtual void setDriver(ExecutorDriver* driver);

  process::Future<int> getFuture();

private:
  process::Owned<SimulatorExecutorProcess> process;
};


class SimulatorExecutorProcess
    : public process::Process<SimulatorExecutorProcess>
{
public:
  SimulatorExecutorProcess(const std::vector<TaskSpec>& taskSpecs);

  virtual void launchTask(const TaskInfo& task);

  virtual void shutdown();

  virtual void sendTerminalUpdate(const TaskID& taskId, TaskState state);

  void setDriver(ExecutorDriver* driver);

  process::Future<int> getFuture();

private:
  // Timer for sending terminal status update.
  process::Timer updateTimer;
  // Executor termination promise.
  process::Promise<int> promise;
  // The currently executing task id (used for 'shutdown').
  TaskID launchedTaskId;
  // Specs for tasks.
  const std::vector<TaskSpec> taskSpecs;
  // The executor driver is owned by the same process as this.
  ExecutorDriver* driver;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __SIMULATOR_EXECUTOR_HPP__
