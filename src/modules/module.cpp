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
  : version(1)
{
  roleVersion["TestModule"]    = "0.22";
  roleVersion["Isolator"]      = "0.22";
  roleVersion["Authenticator"] = "0.22";
  roleVersion["Allocator"]     = "0.22";
}

Try<Nothing> ModuleManager::loadLibraries(string modulePaths)
{
  // load all libs
  // check their MMS version
  // if problem, bail
  foreach (path:module, paths:modules) {
    DynamicLibrary lib;
    Try<Nothing> result = lib.open(path);
    if (!result.isSome()) {
      return Error(result.error());
    }

    Try<void*> symbol = lib.loadSymbol(MODULE_API_VERSION_FUNCTION_STRING);
    if (symbol.isError()) {
      return Error(symbol.error());
    }
    StringFunction* v = (StringFunction*) symbol;
    string libraryVersion = (*v)();
    if (libraryVersion != MODULE_API_VERSION) {
      return Error("Module API version mismatch. " +
                   "Mesos has: " + MODULE_API_VERSION +
                   "library requires: " + libraryVersion);
    }

    // foreach lib:module
    // dlsym module, get return value, which has type Module
    // for each of those, call verification
    symbol = lib.loadSymbol("get" + module + "role");
    if (symbol.isError()) {
      return Error(symbol.error());
    }
    StringFunction* r = (StringFunction*) symbol;
    string role = (*r)();

    if (roleVersion.find(role) == roleVersion.end()) {
      return Error("Unknown module role: " + role);
    }

    if (!isAcceptableRoleVersion(libraryVersion)) {
      return Error("Role version mismatch." +
                   " Mesos supports: >" + roleVersion[role] +
                   " module requires: " + libraryVersion);
    }

    moduleLibMap[module] = lib;
  }
  return Nothing();
}

} // namespace mesos {
