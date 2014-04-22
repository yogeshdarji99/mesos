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

#include <iostream>
#include <iomanip>
#include <list>

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>

#include <process/async.hpp>
#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/id.hpp>
#include <process/io.hpp>
#include <process/reap.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/strings.hpp>
#include <stout/uuid.hpp>

#include "common/type_utils.hpp"

#include "slave/paths.hpp"

#include "slave/containerizer/external_containerizer.hpp"

// Process user environment.
extern char** environ;

using lambda::bind;
using std::list;
using std::map;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using tuples::tuple;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

using state::ExecutorState;
using state::FrameworkState;
using state::RunState;
using state::SlaveState;


ExternalContainerizer::ExternalContainerizer(const Flags& flags)
{
  process = new ExternalContainerizerProcess(flags);
  spawn(process);
}


ExternalContainerizer::~ExternalContainerizer()
{
  terminate(process);
  process::wait(process);
  delete process;
}


Future<Nothing> ExternalContainerizer::recover(
    const Option<state::SlaveState>& state)
{
  return dispatch(process, &ExternalContainerizerProcess::recover, state);
}


Future<Nothing> ExternalContainerizer::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &ExternalContainerizerProcess::launch,
                  containerId,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> ExternalContainerizer::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<std::string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &ExternalContainerizerProcess::launch,
                  containerId,
                  taskInfo,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> ExternalContainerizer::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return dispatch(process,
                  &ExternalContainerizerProcess::update,
                  containerId,
                  resources);
}


Future<ResourceStatistics> ExternalContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(process, &ExternalContainerizerProcess::usage, containerId);
}


Future<containerizer::Termination> ExternalContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(process, &ExternalContainerizerProcess::wait, containerId);
}


void ExternalContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(process, &ExternalContainerizerProcess::destroy, containerId);
}


ExternalContainerizerProcess::ExternalContainerizerProcess(
    const Flags& _flags) : flags(_flags) {}


Future<Nothing> ExternalContainerizerProcess::recover(
    const Option<state::SlaveState>& state)
{
  // TODO(tillt): Consider forwarding the recover command to the
  // external containerizer. For now, recovery should be entirely
  // covered by the slave itself.
  return Nothing();
}


Future<Nothing> ExternalContainerizerProcess::launch(
    const ContainerID& containerId,
    const ExecutorInfo& executorInfo,
    const std::string& directory,
    const Option<std::string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  LOG(INFO) << "Launching container '" << containerId << "'";

  if (containers.contains(containerId)) {
    return Failure("Cannot start already running container '"
      + containerId.value() + "'");
  }

  map<string, string> environment = executorEnvironment(
      executorInfo,
      directory,
      slaveId,
      slavePid,
      checkpoint,
      flags.recovery_timeout);

  if (!flags.hadoop_home.empty()) {
    environment["HADOOP_HOME"] = flags.hadoop_home;
  }

  ExecutorInfo executor;
  executor.CopyFrom(executorInfo);
  CommandInfo* command = executor.mutable_command();

  // When the selected command has no container attached, use the
  // default from the slave startup flags, if available.
  if (!command->has_container()) {
    if (flags.default_container_image.isSome()) {
      command->mutable_container()->set_image(
          flags.default_container_image.get());
    } else {
      LOG(INFO) << "No container specified in task and no default given. "
                << "The external containerizer will have to fill in "
                << "defaults.";
    }
  }

  containerizer::Launch launch;
  launch.mutable_container_id()->CopyFrom(containerId);
  launch.mutable_executor()->CopyFrom(executor);
  launch.mutable_framework_id()->CopyFrom(executor.framework_id());
  launch.set_directory(directory);
  if (user.isSome()) {
    launch.set_user(user.get());
  }
  launch.mutable_slave_id()->CopyFrom(slaveId);
  launch.set_slave_pid(slavePid);
  launch.set_checkpoint(checkpoint);
  launch.set_mesos_executor_path(
      path::join(flags.launcher_dir, "mesos-executor"));

  Sandbox sandbox(directory, user);

  Try<Subprocess> invoked = invoke(
      "launch",
      sandbox,
      launch,
      environment);

  if (invoked.isError()) {
    return Failure("Launch of container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Record the container launch intend.
  containers.put(containerId, Owned<Container>(new Container(sandbox)));

  // Read from the result-pipe and invoke callbacks when reaching EOF.
  return invoked.get().status()
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_launch,
        containerId,
        slaveId,
        checkpoint,
        lambda::_1));
}


Future<Nothing> ExternalContainerizerProcess::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executor,
    const std::string& directory,
    const Option<std::string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  LOG(INFO) << "Launching container '" << containerId << "'";

  if (containers.contains(containerId)) {
    return Failure("Cannot start already running container '"
      + containerId.value() + "'");
  }

  map<string, string> environment = executorEnvironment(
      executor,
      directory,
      slaveId,
      slavePid,
      checkpoint,
      flags.recovery_timeout);

  if (!flags.hadoop_home.empty()) {
    environment["HADOOP_HOME"] = flags.hadoop_home;
  }

  TaskInfo task;
  task.CopyFrom(taskInfo);
  CommandInfo* command = task.has_executor()
    ? task.mutable_executor()->mutable_command()
    : task.mutable_command();
  // When the selected command has no container attached, use the
  // default from the slave startup flags, if available.
  if (!command->has_container()) {
    if (flags.default_container_image.isSome()) {
      command->mutable_container()->set_image(
          flags.default_container_image.get());
    } else {
      LOG(INFO) << "No container specified in task and no default given. "
                << "The external containerizer will have to fill in "
                << "defaults.";
    }
  }

  containerizer::Launch launch;
  launch.mutable_container_id()->CopyFrom(containerId);
  launch.mutable_task()->CopyFrom(task);
  launch.mutable_framework_id()->CopyFrom(executor.framework_id());
  launch.set_directory(directory);
  if (user.isSome()) {
    launch.set_user(user.get());
  }
  launch.mutable_slave_id()->CopyFrom(slaveId);
  launch.set_slave_pid(slavePid);
  launch.set_checkpoint(checkpoint);
  launch.set_mesos_executor_path(
      path::join(flags.launcher_dir, "mesos-executor"));

  Sandbox sandbox(directory, user);

  Try<Subprocess> invoked = invoke(
      "launch",
      sandbox,
      launch,
      environment);

  if (invoked.isError()) {
    return Failure("Launch of container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Record the container launch intend.
  containers.put(containerId, Owned<Container>(new Container(sandbox)));

  // Read from the result-pipe and invoke callbacks when reaching EOF.
  return invoked.get().status()
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_launch,
        containerId,
        slaveId,
        checkpoint,
        lambda::_1));
}


Future<Nothing> ExternalContainerizerProcess::_launch(
    const ContainerID& containerId,
    const SlaveID& slaveId,
    bool checkpoint,
    const Future<Option<int> >& future)
{
  VLOG(1) << "Launch callback triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  Try<Nothing> done = isDone(future);
  if (done.isError()) {
    // 'launch' has failed, we need to tear down everything now.
    terminate(containerId);
    return Failure(done.error());
  }

  // Container launched successfully.
  containers[containerId]->launched.set(Nothing());

  VLOG(1) << "Launch finishing up for container '" << containerId << "'";
  return Nothing();
}


Future<containerizer::Termination> ExternalContainerizerProcess::wait(
    const ContainerID& containerId)
{
  VLOG(1) << "Wait triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  // Defer wait until launch is done.
  return containers[containerId]->launched.future()
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_wait,
        containerId));
}


Future<containerizer::Termination> ExternalContainerizerProcess::_wait(
    const ContainerID& containerId)
{
  VLOG(1) << "Wait continuation triggered on container '"
          << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  Try<Subprocess> invoked = invoke(
      "wait",
      containers[containerId]->sandbox,
      containerId);

  if (invoked.isError()) {
    // 'wait' has failed, we need to tear down everything now.
    terminate(containerId);
    return Failure("Wait on container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  containers[containerId]->pid = invoked.get().pid();

  // Invoke the protobuf::read asynchronously.
  // TODO(tillt): Consider moving protobuf::read into libprocess and
  // making it work fully asynchronously.
  Result<containerizer::Termination>(*p)(int, bool, bool) =
    &::protobuf::read<containerizer::Termination>;

  Future<Result<containerizer::Termination> > future = async(
      p, invoked.get().out(), false, false);

  // Await both, a protobuf Message from the subprocess as well as
  // its exit.
  await(future, invoked.get().status())
    .onAny(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::__wait,
        containerId,
        lambda::_1));

  return containers[containerId]->termination.future();
}

void ExternalContainerizerProcess::__wait(
    const ContainerID& containerId,
    const Future<tuples::tuple<
        Future<Result<containerizer::Termination> >,
        Future<Option<int> > > >& future)
{
  VLOG(1) << "Wait callback triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId << "' not running";
    return;
  }

  Try<containerizer::Termination> termination = result<
      containerizer::Termination>(future);
  if (termination.isError()) {
    // 'wait' has failed, we need to tear down everything now.
    terminate(containerId);
    containers[containerId]->termination.fail(termination.error());
  }

  // Set the promise to alert others waiting on this container.
  containers[containerId]->termination.set(termination.get());

  // The container has been waited on, we can safely cleanup now.
  cleanup(containerId);
}


Future<Nothing> ExternalContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  VLOG(1) << "Update triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "'' not running");
  }

  containers[containerId]->resources = resources;

  containerizer::Update update;
  update.mutable_container_id()->CopyFrom(containerId);
  update.mutable_resources()->CopyFrom(resources);

  Try<Subprocess> invoked = invoke(
      "update",
      containers[containerId]->sandbox,
      update);

  if (invoked.isError()) {
    return Failure("Update of container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  return invoked.get().status()
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_update,
        containerId,
        lambda::_1));
}


Future<Nothing> ExternalContainerizerProcess::_update(
    const ContainerID& containerId,
    const Future<Option<int> >& future)
{
  VLOG(1) << "Update callback triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  Try<Nothing> done = isDone(future);
  if (done.isError()) {
    return Failure(done.error());
  }

  return Nothing();
}


Future<ResourceStatistics> ExternalContainerizerProcess::usage(
    const ContainerID& containerId)
{
  VLOG(1) << "Usage triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "'' not running");
  }

  Try<Subprocess> invoked = invoke(
      "usage",
      containers[containerId]->sandbox,
      containerId);

  if (invoked.isError()) {
    // 'usage' has failed but we keep the container alive for now.
    return Failure("Usage on container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  Result<ResourceStatistics>(*p)(int, bool, bool) =
    &::protobuf::read<ResourceStatistics>;

  Future<Result<ResourceStatistics> > future = async(
      p, invoked.get().out(), false, false);

  // Await both, a protobuf Message from the subprocess as well as
  // its exit.
  return await(future, invoked.get().status())
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_usage,
        containerId,
        lambda::_1));
}


Future<ResourceStatistics> ExternalContainerizerProcess::_usage(
    const ContainerID& containerId,
    const Future<tuples::tuple<
        Future<Result<ResourceStatistics> >,
        Future<Option<int> > > >& future)
{
  VLOG(1) << "Usage callback triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  Try<ResourceStatistics> statistics = result<ResourceStatistics>(future);
  if (statistics.isError()) {
    return Failure(statistics.error());
  }

  VLOG(2) << "Usage result: '" << statistics.get().DebugString() << "'";

  LOG(INFO) << "containerId '" << containerId << "' "
            << "total mem usage "
            << statistics.get().mem_rss_bytes() << " "
            << "total CPU user usage "
            << statistics.get().cpus_user_time_secs() << " "
            << "total CPU system usage "
            << statistics.get().cpus_system_time_secs();

  return statistics.get();
}


void ExternalContainerizerProcess::destroy(const ContainerID& containerId)
{
  VLOG(1) << "Destroy triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId << "' not running";
    return;
  }

  Try<Subprocess> invoked = invoke(
      "destroy",
      containers[containerId]->sandbox,
      containerId);

  if (invoked.isError()) {
    LOG(ERROR) << "Destroy of container '" << containerId
               << "' failed (error: " << invoked.error() << ")";
    terminate(containerId);
    return;
  }

  invoked.get().status()
    .onAny(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_destroy,
        containerId,
        lambda::_1));
}


void ExternalContainerizerProcess::_destroy(
    const ContainerID& containerId,
    const Future<Option<int> >& future)
{
  VLOG(1) << "Destroy callback triggered on container '" << containerId << "'";

  if (!containers.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId.value() << "' not running";
    return;
  }

  Try<Nothing> done = isDone(future);
  if (done.isError()) {
    LOG(ERROR) << "Destroy of container '" << containerId
               << "' failed (error: " << done.error() << ")";
  }

  // Additionally to the optional external destroy-command, we need to
  // terminate the external containerizer's process.
  terminate(containerId);
}


void ExternalContainerizerProcess::cleanup(
    const ContainerID& containerId)
{
  VLOG(1) << "Callback performing final cleanup of running state";

  if (containers.contains(containerId)) {
    containers.erase(containerId);
  } else {
    LOG(WARNING) << "Container '" << containerId << "' not running anymore";
  }
}


void ExternalContainerizerProcess::terminate(const ContainerID& containerId)
{
  if (!containers.contains(containerId)) {
    LOG(WARNING) << "Container '" << containerId << "' not running";
    return;
  }

  Option<pid_t> pid = containers[containerId]->pid;

  // Only containers that are being waited on can be terminated.
  if (pid.isNone()) {
    // If we reached this, launch most likely failed due to some error
    // on the external containerizer's side (e.g. returned non zero on
    // launch).
    LOG(WARNING) << "Container '" << containerId << "' not being waited on";
    cleanup(containerId);
    return;
  }

  // Terminate the containerizer.
  VLOG(2) << "About to send a SIGKILL to containerizer pid: " << pid.get();

  // TODO(tillt): Add graceful termination as soon as we have an
  // accepted way to do that in place.
  Try<list<os::ProcessTree> > trees =
    os::killtree(pid.get(), SIGKILL, true, true);

  if (trees.isError()) {
    LOG(WARNING) << "Failed to kill the process tree rooted at pid "
                 << pid.get() << ": " << trees.error();
    cleanup(containerId);
    return;
  }

  LOG(INFO) << "Killed the following process tree/s:\n"
            << stringify(trees.get());
}


Try<Nothing> ExternalContainerizerProcess::isDone(
    const Future<Option<int> >& future)
{
  if (!future.isReady()) {
    return Error("Status not ready");
  }

  Option<int> status = future.get();
  if (status.isNone()) {
    return Error("External containerizer has no status available");
  }

  // The status is a waitpid-result which has to be checked for SIGNAL
  // based termination before masking out the exit-code.
  if (!WIFEXITED(status.get())) {
    return Error(string("External containerizer terminated by signal ")
      + strsignal(WTERMSIG(status.get())));
  }

  int exitCode = WEXITSTATUS(status.get());
  if (exitCode != 0) {
    return Error("External containerizer failed (exit: "
      + stringify(exitCode) + ")");
  }

  return Nothing();
}


// Post fork, pre exec function.
int setup(const string& directory)
{
  // Put child into its own process session to prevent slave suicide
  // on child process SIGKILL/SIGTERM.
  if (::setsid() == -1) {
    return errno;
  }

  // Re/establish the sandbox conditions for the containerizer.
  if (::chdir(directory.c_str()) == -1) {
    return errno;
  }

  // Sync parent and child process.
  int sync = 0;
  while (::write(STDOUT_FILENO, &sync, sizeof(sync)) == -1
    && errno == EINTR);

  return 0;
}


Try<process::Subprocess> ExternalContainerizerProcess::invoke(
      const string& command,
      const Sandbox& sandbox,
      const google::protobuf::Message& message,
      const map<string, string>& environment)
{
  CHECK(flags.containerizer_path.isSome())
    << "containerizer_path not set";

  VLOG(1) << "Invoking external containerizer for method '" << command << "'";

  // Construct the command to execute.
  string execute = flags.containerizer_path.get() + " " + command;

  VLOG(2) << "calling: [" << execute << "]";
  VLOG(2) << "directory: " << sandbox.directory;
  if(sandbox.user.isSome()) {
    VLOG(2) << "user: " << sandbox.user.get();
  }

  // Re/establish the sandbox conditions for the containerizer.
  if (sandbox.user.isSome()) {
    Try<Nothing> chown = os::chown(
        sandbox.user.get(),
        sandbox.directory);
    if (chown.isError()) {
      return Error(string("Failed to chown work directory: ") +
        strerror(errno));
    }
  }

  // Fork exec of external process. Run a chdir and a setsid within
  // the child-context.
  Try<Subprocess> external = process::subprocess(
      execute,
      environment,
      lambda::bind(&setup, sandbox.directory));

  if (external.isError()) {
    return Error(string("Failed to execute external containerizer: ")
      + external.error());
  }

  // Sync parent and child process to make sure we have done the
  // setsid within the child context before continuing.
  int sync;
  while (::read(external.get().out(), &sync, sizeof(sync)) == -1
    && errno == EINTR);

  // Set stderr into non-blocking mode.
  Try<Nothing> nonblock = os::nonblock(external.get().err());
  if (nonblock.isError()) {
    return Error("Failed to accept nonblock (error: " + nonblock.error()
      + ")");
  }

  // We are not setting stdin or stdout into non-blocking mode as
  // protobuf::read / write do currently not support it.

  // Redirect output (stderr) from the external containerizer to log
  // file in the executor work directory, chown'ing it if a user is
  // specified.
  Try<int> err = os::open(
      path::join(sandbox.directory, "stderr"),
      O_WRONLY | O_CREAT | O_APPEND | O_NONBLOCK,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

  if (err.isError()) {
    return Error("Failed to redirect stderr: " + err.error());
  }

  io::splice(external.get().err(), err.get())
    .onAny(bind(&os::close, err.get()));

  // Transmit protobuf data via stdout towards the external
  // containerizer. Each message is prefixed by its total size.
  Try<Nothing> w = ::protobuf::write(external.get().in(), message);
  if (w.isError()) {
    return Error("Failed to write protobuf to pipe (error: "
                + w.error() + ")");
  }

  VLOG(2) << "Subprocess pid: " << external.get().pid() << ", "
          << "output pipe: " << external.get().out();

  return external;
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
