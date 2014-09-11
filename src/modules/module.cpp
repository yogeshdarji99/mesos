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
#include <hashmap>

#include "module.hpp"

using std::string;
using std::hashmap;

namespace mesos {

#define MODULE_API_VERSION_FUNCTION_STRING \
  #MODULE_API_VERSION_FUNCTION

typedef string(*StringFunction)();

ModuleManager::ModuleManager()
{
  roleToVersion["TestModule"]    = "0.22";
  roleToVersion["Isolator"]      = "0.22";
  roleToVersion["Authenticator"] = "0.22";
  roleToVersion["Allocator"]     = "0.22";
}

Try<string> ModuleManager::callStringFunction(DynamicLibrary *lib,
                                              string functionName)
{
  Try<void*> symbol = lib->loadSymbol(functionName);
  if (symbol.isError()) {
    return Error(symbol.error());
  }
  StringFunction* v = (StringFunction*) symbol;
  return (*v)();
}

Try<DynamicLibrary*> ModuleManager::loadModuleLibrary(string path)
{
  DynamicLibrary *lib = new DynamicLibrary();
  Try<Nothing> result = lib->open(path);
  if (!result.isSome()) {
    return Error(result.error());
  }

  Try<string> libraryVersion =
    callStringFunction(lib, MODULE_API_VERSION_FUNCTION_STRING);
  if (libraryVersion.isError()) {
    return libraryVersion.error();
  }
  if (libraryVersion != MODULE_API_VERSION) {
    return Error("Module API version mismatch. " +
                 "Mesos has: " + MODULE_API_VERSION +
                 "library requires: " + libraryVersion);
  }
  return lib;
}

Try<Nothing> ModuleManager::verifyModuleRole(string module, DynamicLibrary *lib)
{
  Try<string> role = callStringFunction(lib, "get" + module + "Role");
  if (role.isError()) {
    return role.error();
  }
  if (roleToVersion.find(role) == roleToVersion.end()) {
    return Error("Unknown module role: " + role);
  }

  if (libraryVersion != roleToVersion[role]) {
    return Error("Role version mismatch." +
                 " Mesos supports: >" + roleToVersion[role] +
                 " module requires: " + libraryVersion);
  }
  return Nothing();
}

Try<Nothing> ModuleManager::loadLibraries(string modulePaths)
{
  // load all libs
  // check their MMS version
  // if problem, bail
  foreach (path:module, paths:modules) {
    DynamicLibrary *lib = loadModuleLibrary(path);

    // foreach lib:module
    // dlsym module, get return value, which has type Module
    // for each of those, call verification
    verifyModuleRole(lib, module);

    moduleToDynLib[module] = lib;
  }
  return Nothing();
}

} // namespace mesos {
