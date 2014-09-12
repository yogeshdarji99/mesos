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

#include <string>
#include <vector>

#include <stout/hashmap.hpp>
#include <stout/strings.hpp>
#include <stout/numify.hpp>
#include <process/check.hpp>

#include <mesos/module.hpp>

#include "module/module_manager.hpp"

using std::string;
using std::vector;

namespace mesos {

ModuleManager::ModuleManager()
{
  roleToVersion["TestModule"]    = "0.21.0";
  roleToVersion["Isolator"]      = "0.21.0";
  roleToVersion["Authenticator"] = "0.21.0";
  roleToVersion["Allocator"]     = "0.21.0";

// mesos           library      result
// 0.18(0.18)      0.18         FINE
// 0.18(0.18)      0.19         NOT FINE
// 0.19(0.18)      0.18         FINE
// 0.19(0.19)      0.18         NOT FINE
}

Try<DynamicLibrary*> ModuleManager::loadModuleLibrary(string path)
{
  DynamicLibrary *lib = new DynamicLibrary();
  Try<Nothing> result = lib->open(path);
  if (!result.isSome()) {
    return Error(result.error());
  }

  Try<const char*> apiVersionStr =
    callFunction<const char*>(lib, MODULE_API_VERSION_FUNCTION_STRING);
  if (apiVersionStr.isError()) {
    return Error(apiVersionStr.error());
  }
  string apiVersion(apiVersionStr.get());
  if (apiVersion != MODULE_API_VERSION) {
    return Error("Module API version mismatch. "
                 "Mesos has: " MODULE_API_VERSION ", "
                 "library requires: " + apiVersion);
  }
  return lib;
}

Try<Nothing> ModuleManager::verifyModuleRole(DynamicLibrary *lib, string module)
{
  Try<const char*> roleStr =
    callFunction<const char*>(lib,
                              MESOS_GET_MODULE_ROLE_FUNCTION_STRING(module));
  if (roleStr.isError()) {
    return Error(roleStr.error());
  }
  string role(roleStr.get());
  if (!roleToVersion.contains(role)) {
    return Error("Unknown module role: " + role);
  }

  Try<const char*> libraryMesosVersionStr =
    callFunction<const char*>(lib, MESOS_VERSION_FUNCTION_STRING);
  if (libraryMesosVersionStr.isError()) {
    return Error(libraryMesosVersionStr.error());
  }
  string libraryMesosVersion(libraryMesosVersionStr.get());

  // TODO: Replace the '!=' check with '<' check.
  if (libraryMesosVersion != roleToVersion[role]) {
    return Error("Role version mismatch: " + role +
                 " supported by Mesos with version >=" +
                 roleToVersion[role] +
                 ", but module is compiled with " +
                 libraryMesosVersion);
  }
  return Nothing();
}

Try<Nothing> ModuleManager::loadLibraries(string modulePaths)
{
  vector<string> entries = strings::split(modulePaths, ",");
  foreach (string entry, entries) {
    vector<string> tokens = strings::split(entry, ":");
    if (tokens.size() > 1) {
      string path = tokens[0];
      Try<DynamicLibrary*> lib = loadModuleLibrary(path);
      if (lib.isError()) {
        return Error(lib.error());
      }
      for (size_t i = 1; i < tokens.size(); i++) {
        string module = tokens[i];
        Try<Nothing> result = verifyModuleRole(lib.get(), module);
        if (result.isError()) {
          return Error(result.error());
        }
        moduleToDynamicLibrary[module] = lib.get();
      }
    }
  }
  return Nothing();
}

} // namespace mesos {
