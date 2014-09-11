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
#include <stout/numify.hpp>

#include "strings.hpp"

#include "modules/module.hpp"
#include "modules/module_manager.hpp"

using std::string;
using std::vector;

namespace mesos {

#define MODULE_API_VERSION_FUNCTION_STRING \
  #MODULE_API_VERSION_FUNCTION

#define MESOS_VERSION_FUNCTION_STRING \
  #MESOS_VERSION_FUNCTION

ModuleManager::ModuleManager()
{
  roleToVersion["TestModule"]    = "0.22";
  roleToVersion["Isolator"]      = "0.22";
  roleToVersion["Authenticator"] = "0.22";
  roleToVersion["Allocator"]     = "0.22";

// mesos           library      result
// 0.18(0.18)      0.18         FINE
// 0.18(0.18)      0.19         NOT FINE
// 0.19(0.18)      0.18         FINE
// 0.19(0.19)      0.18         NOT FINE
}

template<typename T>
static Try<T> callFunction(DynamicLibrary *lib, string functionName)
{
  Try<void*> symbol = lib->loadSymbol(functionName);
  if (symbol.isError()) {
    return Error(symbol.error());
  }
  T (*v)() = (T (*)()) symbol;
  return (*v)();
}

Try<DynamicLibrary*> ModuleManager::loadModuleLibrary(string path)
{
  DynamicLibrary *lib = new DynamicLibrary();
  Try<Nothing> result = lib->open(path);
  if (!result.isSome()) {
    return Error(result.error());
  }

  Try<string> apiVersion =
    callFunction<string>(lib, MODULE_API_VERSION_FUNCTION_STRING);
  if (apiVersion.isError()) {
    return Error(apiVersion.error();
  }
  if (apiVersion != MODULE_API_VERSION) {
    return Error("Module API version mismatch. " +
                 "Mesos has: " + MODULE_API_VERSION +
                 "library requires: " + apiVersion);
  }
  return lib;
}

Try<Nothing> ModuleManager::verifyModuleRole(DynamicLibrary *lib, string module)
{
  Try<string> role = callFunction<string>(lib, "get" + module + "Role");
  if (role.isError()) {
    return Error(role.error());
  }
  if (!roleToVersion.contains(role.get())) {
    return Error("Unknown module role: " + role.get());
  }

  Try<string> libraryMesosVersion =
    callFunction<string>(lib, MESOS_VERSION_FUNCTION_STRING);
  if (libraryMesosVersion.isError()) {
    return Error(libraryMesosVersion.error());
  }

  if (numify<double>(libraryMesosVersion.get()) >=
      numify<double>(roleToVersion[role.get()])) {
    return Error("Role version mismatch: " + role.get() +
                 " supported by Mesos with version >=" +
                 roleToVersion[role.get()] +
                 ", but module is compiled with " +
                 libraryMesosVersion.get());
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
      for (size_t i = 1; i < tokens.size()) {
        string module = tokens[i];
        Try<Nothing> result = verifyModuleRole(lib.get(), module);
        if (result.isError()) {
          return Error(result.error());
        }
        moduleToDynLib[module] = lib.get();
      }
    }
  }
  return Nothing();
}

template<typename Role>
Try<Role*> loadModule(std::string moduleName)
{
  Option<DynamicLibrary*> lib = moduleToDynamicLibrary[moduleName];
  ASSERT_SOME(lib);

  Try<Role*> instance =
      callFunction<Role*>(lib.get(), "create" + moduleName + "Instance");
  if (instance.isError()) {
    return Error(instance.error());
  }
  return instance;
}

} // namespace mesos {
