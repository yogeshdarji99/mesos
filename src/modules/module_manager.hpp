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

#ifndef __MODULE_MANAGER_HPP__
#define __MODULE_MANAGER_HPP__

#include <boost/noncopyable.hpp>

#include <stout/dynamiclibrary.hpp>
#include <stout/lambda.hpp>
#include <stout/memory.hpp>

#include <mesos/mesos.hpp>
#include <mesos/mesos.hpp>

namespace mesos {

/**
 * Mesos module loading.
 * Phases:
 * 1. Load dynamic libraries that contain modules.
 * 2. Verify versions and compatibilities.
 *   a) Library compatibility.
 *   b) Module compatibility.
 * 3. Instantiate singleton per module.
 * 4. Bind reference to use case.
 */
class ModuleManager {
public:
  ModuleManager();

  /**
   * Phase 1 and 2.
   */
  Try<Nothing> loadLibraries(std::string modulePath);

private:
  hashmap<std::string, DynamicLibrary*> moduleToDynLib;
  hashmap<std::string, std::string> roleToVersion;

};


} // namespace mesos {

#endif // __MODULE_MANAGER_HPP__
