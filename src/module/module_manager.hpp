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

#include <string>
#include <stout/strings.hpp>
#include <stout/hashmap.hpp>
#include <stout/dynamiclibrary.hpp>
#include <process/check.hpp>

#include <mesos/mesos.hpp>

#define STRINGIFY(x) #x
#define EXPAND_AND_STRINGIFY(x) STRINGIFY(x)

#define MODULE_API_VERSION_FUNCTION_STRING \
  EXPAND_AND_STRINGIFY(MODULE_API_VERSION_FUNCTION)

#define MESOS_VERSION_FUNCTION_STRING \
  EXPAND_AND_STRINGIFY(MESOS_VERSION_FUNCTION)

#define MESOS_GET_MODULE_ROLE_FUNCTION_STRING(moduleName) \
  "mesos_get_role_" + moduleName

#define MESOS_CREATE_MODULE_FUNCTION_STRING(moduleName) \
  "mesos_create_module_" + moduleName

namespace mesos {

class MesosVersion
{
  public:
    MesosVersion(std::string str)
      : _versionStr(str) {
        std::vector<std::string> versions = strings::split(str, ".");
        CHECK(versions.size() == 3);
        _major = numify<int>(versions[0]).get();
        _minor = numify<int>(versions[1]).get();
        _patch = numify<int>(versions[2]).get();
      }

    std::string str() const { return _versionStr; }
    unsigned major() const { return _major; }
    unsigned minor() const { return _minor; }
    unsigned patch() const { return _patch; }

    bool operator==(const MesosVersion &o) const {
      return _major == o._major &&
        _minor == o._minor &&
        _patch == o._patch;
    }

    bool operator<(const MesosVersion &o) const {
      return _major < o._major ||
        (_major == o._major && _minor < o._minor) ||
        (_major == o._major && _minor == o._minor && _patch < o._patch);
    }

    bool operator>(const MesosVersion &o) const {
      return _major > o._major ||
        (_major == o._major && _minor > o._minor) ||
        (_major == o._major && _minor == o._minor && _patch > o._patch);
    }


    bool operator<=(const MesosVersion &o) const {
      return o > *this;
    }

    bool operator>=(const MesosVersion &o) const {
      return o < *this;
    }

  private:
    std::string _versionStr;
    unsigned _major;
    unsigned _minor;
    unsigned _patch;
};

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

  Try<Nothing> loadLibraries(std::string modulePath);

  template<typename Role>
  Try<Role*> loadModule(std::string moduleName)
  {
    Option<DynamicLibrary*> lib = moduleToDynamicLibrary[moduleName];
    CHECK_SOME(lib);

    Try<Role*> instance =
      callFunction<Role*>(lib.get(),
                          MESOS_CREATE_MODULE_FUNCTION_STRING(moduleName));
    if (instance.isError()) {
      return Error(instance.error());
    }
    return instance;
  }


  bool containsModule(std::string moduleName) const {
    return moduleToDynamicLibrary.contains(moduleName);
  }

private:
  Try<DynamicLibrary*> loadModuleLibrary(std::string path);
  Try<Nothing> verifyModuleRole(DynamicLibrary *lib, std::string module);

  template<typename T>
  Try<T> callFunction(DynamicLibrary *lib, std::string functionName)
  {
    Try<void*> symbol = lib->loadSymbol(functionName);
    if (symbol.isError()) {
      return Error(symbol.error());
    }
    T (*v)() = (T (*)()) symbol.get();
    return (*v)();
  }

  hashmap<std::string, DynamicLibrary*> moduleToDynamicLibrary;
  hashmap<std::string, MesosVersion*> roleToVersion;
};


} // namespace mesos {

#endif // __MODULE_MANAGER_HPP__
