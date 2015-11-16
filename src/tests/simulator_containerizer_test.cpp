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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <mesos/executor.hpp>

#include "master/master.hpp"

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/simulator/containerizer.hpp"
#include "slave/containerizer/simulator/executor.hpp"
#include "slave/slave.hpp"

#include "tests/mesos.hpp"

using namespace process;

using mesos::internal::master::Master;

using std::string;
using std::vector;

using testing::_;
using testing::InSequence;
using testing::Invoke;
using testing::Property;
using testing::Return;
using testing::DoDefault;

namespace mesos {
namespace internal {
namespace tests {
using namespace slave;

class SimulatorContainerizerTest : public MesosTest
{
};

class SimulatorExecutorTest : public MesosTest
{
};

class MockSimulatorContainerizer : public SimulatorContainerizer
{
public:
  MOCK_METHOD8(launch,
               process::Future<bool>(const ContainerID&,
                                     const TaskInfo&,
                                     const ExecutorInfo&,
                                     const std::string&,
                                     const Option<string>&,
                                     const SlaveID&,
                                     const process::PID<Slave>&,
                                     bool checkpoint));

  MockSimulatorContainerizer(const vector<TaskSpec>& specs)
    : SimulatorContainerizer(specs)
  {
    // Set up defaults for mocked methods.
    // NOTE: See TestContainerizer::setup for why we use
    // 'EXPECT_CALL' and 'WillRepeatedly' here instead of
    // 'ON_CALL' and 'WillByDefault'.
    EXPECT_CALL(*this, launch(_, _, _, _, _, _, _, _))
      .WillRepeatedly(Invoke(this, &MockSimulatorContainerizer::_launch));
  }

  process::Future<bool> _launch(const ContainerID& containerId,
                                const TaskInfo& taskInfo,
                                const ExecutorInfo& executorInfo,
                                const string& directory,
                                const Option<string>& user,
                                const SlaveID& slaveId,
                                const PID<Slave>& slavePid,
                                bool checkpoint)
  {
    return SimulatorContainerizer::launch(containerId,
                                          taskInfo,
                                          executorInfo,
                                          directory,
                                          user,
                                          slaveId,
                                          slavePid,
                                          checkpoint);
  }
};

class MockMesosExecutorDriver : public ExecutorDriver
{
public:
  MOCK_METHOD1(sendStatusUpdate, Status(const TaskStatus&));

  Status start() { return DRIVER_RUNNING; }

  // Stops the executor driver.
  Status stop() { return DRIVER_RUNNING; }

  // Aborts the driver so that no more callbacks can be made to the
  // executor. The semantics of abort and stop have deliberately been
  // separated so that code can detect an aborted driver (i.e., via
  // the return status of ExecutorDriver::join, see below), and
  // instantiate and start another driver if desired (from within the
  // same process ... although this functionality is currently not
  // supported for executors).
  Status abort() { return DRIVER_RUNNING; }

  // Waits for the driver to be stopped or aborted, possibly
  // _blocking_ the current thread indefinitely. The return status of
  // this function can be used to determine if the driver was aborted
  // (see mesos.proto for a description of Status).
  Status join() { return DRIVER_RUNNING; }

  // Starts and immediately joins (i.e., blocks on) the driver.
  Status run() { return DRIVER_RUNNING; }

  Status sendFrameworkMessage(const std::string& data)
  {
    return DRIVER_RUNNING;
  }

  MockMesosExecutorDriver()
  {
    ON_CALL(*this, sendStatusUpdate(_))
      .WillByDefault(Return(DRIVER_RUNNING));
  }
};

class TaskSpecTest : public MesosTest
{
};

TEST_F(TaskSpecTest, MatchTaskID)
{
  TaskSpec spec("SOME_PREFIX.*", 0.1, 10, 5);
  TaskID taskID;

  taskID.set_value("SOME_PREFIX_whatever");
  EXPECT_EQ(spec.match(taskID), true);

  taskID.set_value("SOME_PREFIX");
  EXPECT_EQ(spec.match(taskID), true);

  taskID.set_value("SOME_");
  EXPECT_EQ(spec.match(taskID), false);

  taskID.set_value("whatever_SOME_PREFIX");
  EXPECT_EQ(spec.match(taskID), false);

  taskID.set_value("I_am_totally_random");
  EXPECT_EQ(spec.match(taskID), false);
}

TEST_F(TaskSpecTest, FailureAndLifetimeDistribution)
{
  const string& pattern(".*");
  const double failureRate = 0.1;
  const double lengthMean = 10;
  const double lengthSTD = 5;
  TaskSpec spec(pattern, failureRate, lengthMean, lengthSTD);

  int numTasksFinished = 0, numTasksFailed = 0;

  vector<double> lifetimes;
  int total = 1000000;
  for (int i = 0; i < total; i++) {
    TaskLifetime lt = spec.getLifetime();
    if (lt.state == TASK_FINISHED) {
      numTasksFinished++;
    } else {
      numTasksFailed++;
    }
    lifetimes.push_back(lt.lifetime);
  }
  int shouldFail = total * failureRate;
  EXPECT_LT((double)(std::abs(numTasksFailed - shouldFail)) / shouldFail, 0.1);

  // Compute the mean and std
  double sum = std::accumulate(lifetimes.begin(), lifetimes.end(), 0.0);
  double mean = sum / lifetimes.size();
  double sq_sum = std::inner_product(
    lifetimes.begin(), lifetimes.end(), lifetimes.begin(), 0.0);
  double stdev = std::sqrt(sq_sum / lifetimes.size() - mean * mean);

  EXPECT_NEAR(mean, lengthMean, 0.1);
  EXPECT_NEAR(stdev, lengthSTD, 0.1);
}

TEST_F(SimulatorContainerizerTest, FailOnNonexistentConfig)
{
  string cwd = os::getcwd();

  slave::Flags flags;
  flags.simulator_config = path::join(cwd, "this_file_doesnt_exist");

  Try<SimulatorContainerizer*> containerizer =
    SimulatorContainerizer::create(flags);
  ASSERT_ERROR(containerizer);
}

TEST_F(SimulatorContainerizerTest, WaitExitsAfterDestroyIsCalled)
{
  Try<PID<Master> > master = this->StartMaster();
  ASSERT_SOME(master);

  Flags testFlags;

  slave::Flags flags = this->CreateSlaveFlags();

  Try<SimulatorContainerizer*> _containerizer =
    SimulatorContainerizer::create(flags);
  ASSERT_SOME(_containerizer);

  SimulatorContainerizer* containerizer = _containerizer.get();

  Try<PID<Slave> > slave = this->StartSlave(containerizer, flags);
  ASSERT_SOME(slave);

  string cwd = os::getcwd(); // We're inside a temporary sandbox.

  ContainerID containerID;
  containerID.set_value("test_container");

  process::Future<bool> launch =
    containerizer->launch(containerID,
                          CREATE_EXECUTOR_INFO("executor", "sleep 1"),
                          cwd,
                          None(),
                          SlaveID(),
                          slave.get(),
                          false);

  // Wait for the launch to complete.
  AWAIT_READY(launch);

  auto termination = containerizer->wait(containerID);
  Clock::pause();
  Clock::advance(Seconds(1));
  Clock::settle();
  // shouldn't be ready, since we haven't destroyed the container
  Clock::resume();
  EXPECT_FALSE(termination.isReady());

  containerizer->destroy(containerID);
  AWAIT_READY(termination);

  Shutdown();
}

TEST_F(SimulatorExecutorTest, SendStatusUpdateNoConfig)
{
  std::vector<TaskSpec> taskSpecs;
  SimulatorExecutor executor(taskSpecs);

  MockMesosExecutorDriver driver;
  executor.setDriver(&driver);

  TaskInfo task;
  task.set_name("test_task_id");

  // Should first send a TASK_RUNNING update, then a TASK_FINISHED update
  {
    InSequence dummy;

    TaskID taskID(task.task_id());
    EXPECT_CALL(driver,
                sendStatusUpdate(Property(&TaskStatus::state, TASK_RUNNING)));
    EXPECT_CALL(driver,
                sendStatusUpdate(Property(&TaskStatus::state, TASK_FINISHED)));
    executor.launchTask(&driver, task);
    Clock::pause();
    Clock::advance(Seconds(
        SimulatorExecutorProcess::DEFAULT_TASK_LIFETIME_SECS + 1));
    Clock::settle();
    Clock::resume();
  }

  Shutdown();
}

TEST_F(SimulatorExecutorTest, SendStatusUpdateWithConfig)
{
  // In this case, all tasks should finish in 10 seconds
  TaskSpec spec("test_.*", 0, 10, 0);
  std::vector<TaskSpec> taskSpecs;
  taskSpecs.push_back(spec);
  SimulatorExecutor executor(taskSpecs);

  MockMesosExecutorDriver driver;
  executor.setDriver(&driver);

  for (int i = 0; i < 100; i++) {
    TaskInfo task;
    task.set_name("test_task_id");
    {
      InSequence dummy;

      TaskID taskID(task.task_id());
      EXPECT_CALL(driver,
                  sendStatusUpdate(Property(&TaskStatus::state,
                      TASK_RUNNING)));
      EXPECT_CALL(driver,
                  sendStatusUpdate(Property(&TaskStatus::state,
                      TASK_FINISHED)));
      executor.launchTask(&driver, task);
      Clock::pause();
      Clock::advance(Seconds(10 + 1));
      Clock::settle();
      Clock::resume();
    }
  }

  Shutdown();
}

} //  namespace tests {
} //  namespace internal {
} //  namespace mesos {
