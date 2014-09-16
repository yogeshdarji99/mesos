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
#include <stout/strings.hpp>
#include <stout/stringify.hpp>
#include <stout/version.hpp>
#include <process/check.hpp>

#include <mesos/module.hpp>

#include "module/module_manager.hpp"

using std::string;
using std::vector;

namespace mesos {
namespace internal {


ModuleManager* ModuleManager::instance()
{
  static ModuleManager moduleManager;
  return &moduleManager;
}


ModuleManager::ModuleManager()
{
// ATTENTION: Every time a Mesos developer breaks compatibility
// with a module role type, this table needs to be updated.
// Specifically, the version value in the entry corresponding to the role
// needs to be set to the Mesos version that affects the current change.
// Typically that should be the version currently under development.

  roleToVersion["TestModule"]    = "0.21.0";
  roleToVersion["Isolator"]      = "0.21.0";
  roleToVersion["Authenticator"] = "0.21.0";
  roleToVersion["Allocator"]     = "0.21.0";

// What happens then when Mesos is built with a certain version,
// 'roleToVersion' states a certain other minimum version,
// and a module library is built against "module.hpp"
// belonging to yet another Mesos version?
//
// Mesos can admit libraries built against earlier versions of itself
// by stating so explicitly in 'roleToVersion'.
// If a library is built with a Mesos version greater than or equal to
// the one stated in 'roleToVersion', it passes this verification step.
// Otherwise it is rejected when attempting to load it.
//
// Here are some examples:
//
// Mesos   roleToVersion    library    modules loadable?
// 0.18.0      0.18.0       0.18.0          YES
// 0.29.0      0.18.0       0.18.0          YES
// 0.29.0      0.18.0       0.21.0          YES
// 0.18.0      0.18.0       0.29.0          NO
// 0.29.0      0.21.0       0.18.0          NO
// 0.29.0      0.29.0       0.18.0          NO

// ATTENTION: This mechanism only protects the interfaces of modules, not how
// they maintain functional compatibility with Mesos and among each other.
// This is covered by their own "isCompatible" call.
}


Try<DynamicLibrary*> ModuleManager::loadModuleLibrary(string path)
{
  DynamicLibrary *lib = new DynamicLibrary();
  Try<Nothing> result = lib->open(path);
  if (!result.isSome()) {
    return Error(result.error());
  }

  Try<const char*> apiVersionStr =
    callFunction<const char*>(lib, MESOS_MODULE_API_VERSION_FUNCTION_STRING);
  if (apiVersionStr.isError()) {
    return Error(apiVersionStr.error());
  }
  string apiVersion(apiVersionStr.get());
  if (apiVersion != MESOS_MODULE_API_VERSION) {
    return Error("Module API version mismatch. "
                 "Mesos has: " MESOS_MODULE_API_VERSION ", "
                 "library requires: " + apiVersion);
  }
  return lib;
}


Try<Nothing> ModuleManager::verifyModuleRole(DynamicLibrary *lib, string module)
{
  Try<const char*> r =
    callFunction<const char*>(lib,
                              MESOS_GET_MODULE_ROLE_FUNCTION_STRING(module));
  if (r.isError()) {
    return Error(r.error());
  }
  string role(r.get());
  if (!instance()->roleToVersion.contains(role)) {
    return Error("Unknown module role: " + role);
  }

  Try<const char*> v =
    callFunction<const char*>(lib, MESOS_VERSION_FUNCTION_STRING);
  if (v.isError()) {
    return Error(v.error());
  }
  Try<Version> libraryMesosVersion = Version::parse(v.get());
  if (libraryMesosVersion.isError()) {
    return Error(libraryMesosVersion.error());
  }
  Try<Version> mesosVersion = Version::parse(MESOS_VERSION);
  if (mesosVersion.isError()) {
    return Error(mesosVersion.error());
  }

  Try<void*> symbol =
      lib->loadSymbol(MESOS_IS_MODULE_COMPATIBILE_FUNCTION_STRING(module));
  if (symbol.isError()) {
    if (libraryMesosVersion.get() != mesosVersion.get()) {
      return Error("Mesos has version " + stringify(mesosVersion.get()) +
                   ", but module is compiled with version " +
                   stringify(libraryMesosVersion.get()));
    }
    return Nothing();
  }

  if (libraryMesosVersion.get() >= mesosVersion.get()) {
    return Error("Mesos has version " + stringify(mesosVersion.get()) +
                 ", but module is compiled with version " +
                 stringify(libraryMesosVersion.get()));
  }

  Try<bool> result =
    callFunction<bool>(lib,
        MESOS_IS_MODULE_COMPATIBILE_FUNCTION_STRING(module));
  if (result.isError()) {
    return Error(result.error());
  }
  if (!result.get()) {
    return Error("Module " + module + "has determined to be incompatible");
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
        instance()->moduleToDynamicLibrary[module] = lib.get();
      }
    }
  }
  return Nothing();
}

} // namespace internal {
} // namespace mesos {
