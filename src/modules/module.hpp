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

#ifndef __MODULE_HPP__
#define __MODULE_HPP__

#include <string>
#include <mesos/mesos.hpp>

namespace mesos {

// Module implementation utilities:

#define MODULE_API_VERSION "1"

#define MESOS_VERSION_FUNCTION mesosVersion
#define MODULE_API_VERSION_FUNCTION moduleApiVersion

#define DEFINE_VERSIONS() \
  std::string MESOS_VERSION_FUNCTION() { return MESOS_VERSION; } \
  std::string MODULE_API_VERSION_FUNCTION() { return MODULE_API_VERSION; }

#define DEFINE_MODULE(role, name) \
  std::string get##name##Role() { return #role; } \
  role create##name##Instance()


// class ModuleInfo
// {
// public:
//   ModuleRole role;
// 
//   // For dependency resolution only.
//   int version;
//   vector<std::string> depends;
//   vector<std::string> provides;
//   vector<std::string> conflicts;
// 
// };

} // namespace mesos {

#endif // __MODULE_HPP__
