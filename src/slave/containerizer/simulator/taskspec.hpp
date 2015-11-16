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

#ifndef __SIMULATOR_TASK_SPEC_HPP__
#define __SIMULATOR_TASK_SPEC_HPP__

#include <regex>
#include <random>
#include <chrono>

#include "mesos/mesos.hpp"

namespace mesos {
namespace internal {
namespace slave {

struct TaskLifetime {
  TaskState state;
  double lifetime;
};

class TaskSpec
{
public:
  TaskSpec(const std::string& _pattern,
           double _failureRate,
           double lengthMean,
           double lengthSTD)
    : pattern(std::regex(_pattern)),
      failureRate(_failureRate),
      generator(std::default_random_engine(
        std::chrono::system_clock::now().time_since_epoch().count())),
      lengthDistribution(lengthMean, lengthSTD){}

  virtual bool match(const TaskID& taskID) const
  {
    return std::regex_match(taskID.value(), pattern);
  }

  virtual TaskLifetime getLifetime() const
  {
    TaskLifetime tl;
    tl.lifetime = lengthDistribution(generator);
    if (failureDistribution(generator) < failureRate) {
      tl.state = TASK_FAILED;
    } else {
      tl.state = TASK_FINISHED;
    }
    return tl;
  }

private:
  std::regex pattern;
  double failureRate;
  mutable std::default_random_engine generator;
  mutable std::normal_distribution<double> lengthDistribution;
  mutable std::uniform_real_distribution<double> failureDistribution;
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {


#endif
