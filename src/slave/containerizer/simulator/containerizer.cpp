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

#include <fstream>
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

#include "module/manager.hpp"

#include "slave/paths.hpp"
#include "slave/slave.hpp"

#include "slave/containerizer/containerizer.hpp"

#include "slave/containerizer/simulator/taskspec.hpp"
#include "slave/containerizer/simulator/containerizer.hpp"

using std::list;
using std::map;
using std::string;
using std::vector;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

using mesos::modules::ModuleManager;

using mesos::slave::Isolator;

using state::SlaveState;
using state::FrameworkState;
using state::ExecutorState;
using state::RunState;

Try<SimulatorContainerizer*> SimulatorContainerizer::create(const Flags& flags)
{
  vector<TaskSpec> taskSpecs;
  if (flags.simulator_config.isNone()) {
    return new SimulatorContainerizer(taskSpecs);
  }

  // read the task specifications from the config file
  std::ifstream configFile(flags.simulator_config.get());
  if (!configFile.good()) {
    return Error("Simulator config file could not be opened: " +
                 flags.simulator_config.get());
  }

  std::stringstream buffer;
  buffer << configFile.rdbuf();

  Try<JSON::Value> c = JSON::parse(buffer.str());
  if (c.isError()) {
    return Error("Unable to parse config file; not valid JSON: " + c.error());
  }

  auto config = c.get().as<JSON::Object>();

  auto taskSpecsArr = config.find<JSON::Array>("task_specs");
  if (taskSpecsArr.isNone()) {
    return Error("No task_specs key found in simulator config");
  } else if (taskSpecsArr.isError()) {
    return Error("Error finding task_specs in simulator config");
  }

  foreach (const JSON::Value& _taskSpecVal, taskSpecsArr.get().values) {
    const auto taskSpecObj = _taskSpecVal.as<JSON::Object>();
    auto pattern = taskSpecObj.find<JSON::String>("pattern");
    if (pattern.isNone()) {
      return Error("A task spec needs to have a 'pattern' field");
    }
    auto failure_rate = taskSpecObj.find<JSON::Number>("failure_rate");
    if (failure_rate.isNone()) {
      return Error("A task spec needs to have a 'failure_rate' field");
    }
    auto length_mean = taskSpecObj.find<JSON::Number>("length_mean");
    if (length_mean.isNone()) {
      return Error("A task spec needs to have a 'length_mean' field");
    }
    auto length_std = taskSpecObj.find<JSON::Number>("length_std");
    if (length_std.isNone()) {
      return Error("A task spec needs to have a 'length_std' field");
    }
    TaskSpec spec(pattern.get().value,
                  failure_rate.get().as<double>(),
                  length_mean.get().as<double>(),
                  length_std.get().as<double>());
    taskSpecs.push_back(spec);
  }
  return new SimulatorContainerizer(taskSpecs);
}


SimulatorContainerizer::SimulatorContainerizer(const vector<TaskSpec>& specs)
    : process(new SimulatorContainerizerProcess(specs))
{
  spawn(process.get());
}


SimulatorContainerizer::~SimulatorContainerizer()
{
  terminate(process.get());
  process::wait(process.get());
}


Future<Nothing> SimulatorContainerizer::recover(
    const Option<state::SlaveState>& state)
{
  return dispatch(
      process.get(), &SimulatorContainerizerProcess::recover, state);
}


Future <bool> SimulatorContainerizer::launch(const ContainerID& containerId,
                                             const ExecutorInfo& executorInfo,
                                             const string& directory,
                                             const Option<string>& user,
                                             const SlaveID& slaveId,
                                             const PID<Slave>& slavePid,
                                             bool checkpoint)
{
  return dispatch(process.get(),
                  &SimulatorContainerizerProcess::launch,
                  containerId,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future <bool> SimulatorContainerizer::launch(const ContainerID& containerId,
                                             const TaskInfo& taskInfo,
                                             const ExecutorInfo& executorInfo,
                                             const string& directory,
                                             const Option<string>& user,
                                             const SlaveID& slaveId,
                                             const PID<Slave>& slavePid,
                                             bool checkpoint)
{
  return dispatch(process.get(),
                  &SimulatorContainerizerProcess::launch,
                  containerId,
                  taskInfo,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> SimulatorContainerizer::update(const ContainerID& containerId,
                                                const Resources& resources)
{
  return dispatch(process.get(),
                  &SimulatorContainerizerProcess::update,
                  containerId,
                  resources);
}


Future<ResourceStatistics> SimulatorContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(
      process.get(), &SimulatorContainerizerProcess::usage, containerId);
}


Future<containerizer::Termination> SimulatorContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(
      process.get(), &SimulatorContainerizerProcess::wait, containerId);
}


void SimulatorContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(process.get(), &SimulatorContainerizerProcess::destroy, containerId);
}


Future<hashset<ContainerID>> SimulatorContainerizer::containers()
{
  return dispatch(process.get(), &SimulatorContainerizerProcess::containers);
}


// Initialize envLock.
std::mutex SimulatorContainerizerProcess::envLock;

SimulatorContainerizerProcess::SimulatorContainerizerProcess(
    const std::vector<TaskSpec>& _taskSpecs)
    : taskSpecs(_taskSpecs) {}


SimulatorContainerizerProcess::~SimulatorContainerizerProcess()
{
  foreachvalue (const Owned<Container>& container, containers_) {
    container->driver->stop();
    container->driver->join();
  }
  containers_.clear();
}


Future<bool> SimulatorContainerizerProcess::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<slave::Slave>& slavePid,
    bool checkpoint)
{
  return _launch(containerId,
                 None(),
                 executorInfo,
                 directory,
                 user,
                 slaveId,
                 slavePid,
                 checkpoint);
}


Future<bool> SimulatorContainerizerProcess::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<slave::Slave>& slavePid,
    bool checkpoint)
{
  return _launch(containerId,
                 taskInfo,
                 executorInfo,
                 directory,
                 user,
                 slaveId,
                 slavePid,
                 checkpoint);
}


bool SimulatorContainerizerProcess::_launch(const ContainerID& containerId,
                                            const Option<TaskInfo>& taskInfo,
                                            const ExecutorInfo& executorInfo,
                                            const string& directory,
                                            const Option<string>& user,
                                            const SlaveID& slaveId,
                                            const PID<slave::Slave>& slavePid,
                                            bool checkpoint)
{
  LOG(INFO) << "Launching task with container ID " << containerId;
  CHECK(!containers_.contains(containerId))
  << "Failed to launch executor " << executorInfo.executor_id()
  << " of framework " << executorInfo.framework_id()
  << " because it is already launched";

  Container * container = new Container();
  container->executor =
      Owned<SimulatorExecutor>(new SimulatorExecutor(taskSpecs));

  // Since we are not launching separate processes, we must synchronize
  // modification and reading of environmental variables.
  {
    std::lock_guard<std::mutex> lock(envLock);
    container->driver = Owned<MesosExecutorDriver>(
        new MesosExecutorDriver(container->executor.get()));

    container->executor->setDriver(container->driver.get());

    // Prepare additional environment variables for the executor.
    // TODO(benh): Need to get flags passed into the TestContainerizer
    // in order to properly use here.
    slave::Flags flags;
    flags.recovery_timeout = Duration::zero();

    // We need to save the original set of environment variables so we
    // can reset the environment after calling 'driver->start()' below.
    hashmap<string, string> original = os::environment();

    const map<string, string> environment = executorEnvironment(
        executorInfo, directory, slaveId, slavePid, checkpoint, flags);

    foreachpair (const string& name, const string variable, environment) {
      os::setenv(name, variable);
    }

    // TODO(benh): Can this be removed and done exlusively in the
    // 'executorEnvironment()' function? There are other places in the
    // code where we do this as well and it's likely we can do this once
    // in 'executorEnvironment()'.
    foreach (const Environment::Variable& variable,
             executorInfo.command().environment().variables()) {
      os::setenv(variable.name(), variable.value());
    }

    os::setenv("MESOS_LOCAL", "1");

    container->driver->start();

    os::unsetenv("MESOS_LOCAL");

    // Unset the environment variables we set by resetting them to their
    // original values and also removing any that were not part of the
    // original environment.
    foreachpair (const string& name, const string& value, original) {
      os::setenv(name, value);
    }

    foreachkey (const string& name, environment) {
      if (!original.contains(name)) {
        os::unsetenv(name);
      }
    }
  }
  // Defer the container destroy until after the task has hit a terminal state.
  Future<int> status = container->executor->getFuture();
  status.onAny(defer(self(), &Self::delayDestroy, containerId));
  // Add the container to the hashmap.
  containers_.put(containerId, Owned<Container>(container));

  return true;
}

void SimulatorContainerizerProcess::delayDestroy(const ContainerID& containerId)
{
  LOG(INFO) << "Delaying destroy for " << TERMINATION_DELAY
  << " seconds";
  // A hack. We want to destroy the executor only after the terminal
  // status update has been acked, but currently there is no way to
  // know when that happens. So, we just delay sending the status
  // update.
  delay(Seconds(TERMINATION_DELAY),
        self(),
        &Self::destroy,
        containerId);
}


void SimulatorContainerizerProcess::destroy(const ContainerID& containerId)
{
  LOG(INFO) << "Destroying container '" << containerId << "'";

  if (containers_.contains(containerId)) {
    Owned<Container> container = containers_[containerId];
    // Stop and join the driver.
    container->driver->stop();
    container->driver->join();

    // Set the termination promise.
    containerizer::Termination termination;
    termination.set_state(TaskState::TASK_KILLED);
    termination.set_message("Killed executor");
    termination.set_status(0);
    container->promise.set(termination);
  }
  containers_.erase(containerId);
}


Future<containerizer::Termination> SimulatorContainerizerProcess::wait(
    const ContainerID& containerId)
{
  // An unknown container is possible for tests where we "drop" the
  // 'launch' in order to verify recovery still works correctly.
  if (!containers_.contains(containerId)) {
    return Failure("Unknown container: " + stringify(containerId));
  }

  return containers_[containerId]->promise.future();
}


Future<Nothing> SimulatorContainerizerProcess::recover(
    const Option<state::SlaveState>& state)
{
  LOG(INFO) << "Recovering containerizer";
  return Nothing();
}


Future<Nothing> SimulatorContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return Nothing();
}


Future<ResourceStatistics> SimulatorContainerizerProcess::usage(
    const ContainerID& containerId)
{
  LOG(INFO) << "Calling unimplemented function 'usage'";
  return Failure("Not implemented yet");
}


Future<hashset<ContainerID>> SimulatorContainerizerProcess::containers()
{
  return containers_.keys();
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
