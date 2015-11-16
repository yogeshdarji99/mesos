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

#include <regex>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/io.hpp>
#include <process/metrics/metrics.hpp>
#include <process/reap.hpp>
#include <process/subprocess.hpp>

#include "common/protobuf_utils.hpp"
#include "mesos/mesos.hpp"

#include <stout/fs.hpp>
#include <stout/os.hpp>
#include <stout/path.hpp>
#include <stout/json.hpp>
#include <stout/duration.hpp>
#include <stout/flags/parse.hpp>
#include <stout/uuid.hpp>

#include "slave/slave.hpp"

#include "slave/containerizer/containerizer.hpp"

#include "slave/containerizer/simulator/taskspec.hpp"
#include "slave/containerizer/simulator/executor.hpp"

using std::list;
using std::map;
using std::string;
using std::vector;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

SimulatorExecutor::SimulatorExecutor(const std::vector <TaskSpec>& taskSpecs)
    : process(new SimulatorExecutorProcess(taskSpecs))
{
  spawn(process.get());
}


SimulatorExecutor::~SimulatorExecutor()
{
  terminate(process.get());
  process::wait(process.get());
}


void SimulatorExecutor::shutdown(ExecutorDriver * driver)
{
  dispatch(process.get(), &SimulatorExecutorProcess::shutdown);
}


void SimulatorExecutor::killTask(ExecutorDriver* driver, const TaskID& taskId)
{
  dispatch(process.get(),
           &SimulatorExecutorProcess::sendTerminalUpdate,
           taskId,
           TASK_KILLED);
}


void SimulatorExecutor::setDriver(ExecutorDriver * driver)
{
  process->setDriver(driver);
}


void SimulatorExecutor::launchTask(ExecutorDriver* driver, const TaskInfo& task)
{
  dispatch(process.get(), &SimulatorExecutorProcess::launchTask, task);
}


Future<int> SimulatorExecutor::getFuture()
{
  return process->getFuture();
}


Future<int> SimulatorExecutorProcess::getFuture()
{
  return promise.future();
}


SimulatorExecutorProcess::SimulatorExecutorProcess(
    const std::vector<TaskSpec>& _taskSpecs)
    : taskSpecs(_taskSpecs) {}


void SimulatorExecutorProcess::launchTask(const TaskInfo& task)
{
  LOG(INFO) << "Launching task " << task.task_id();
  launchedTaskId = task.task_id();
  // Find the matching task specification
  foreach (const TaskSpec& spec, taskSpecs) {
    if (spec.match(task.task_id())) {
      TaskLifetime lifetime = spec.getLifetime();
      if (lifetime.lifetime == 0) {
        // If lifetime is 0, then the task shouldn't be started at
        // all, in which case we send the terminal status update immediately.
        dispatch(self(),
                 &SimulatorExecutorProcess::sendTerminalUpdate,
                 task.task_id(),
                 lifetime.state);
      } else {
        // Have the driver send the status update after delaying for the
        // appropriate amount of time.
        LOG(INFO) << "Sending status TASK_RUNNING for " << task.task_id();
        TaskStatus status;
        status.mutable_task_id()->MergeFrom(task.task_id());
        status.set_state(TASK_RUNNING);
        driver->sendStatusUpdate(status);
        updateTimer = delay(Seconds(lifetime.lifetime),
                            self(),
                            &SimulatorExecutorProcess::sendTerminalUpdate,
                            task.task_id(),
                            lifetime.state);
      }
      return;
    }
  }
}


void SimulatorExecutorProcess::shutdown()
{
  LOG(INFO) << "Shutting down executor";
  sendTerminalUpdate(launchedTaskId, TASK_KILLED);
}


void SimulatorExecutorProcess::sendTerminalUpdate(const TaskID& taskId,
                                                  TaskState state)
{
  // Have the driver send the status update.
  LOG(INFO) << "Sending terminal status " << state << " for task " << taskId;
  TaskStatus status;
  status.mutable_task_id()->MergeFrom(taskId);
  status.set_state(state);
  status.set_message("Terminal state from simulator");
  driver->sendStatusUpdate(status);
  promise.set(0);
}


void SimulatorExecutorProcess::setDriver(ExecutorDriver * driver_)
{
  driver = driver_;
}

} // namespace slave {
} // namespace internal {
} // namespace mesos {
