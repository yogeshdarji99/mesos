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

#include <boost/noncopyable.hpp>

#include <stout/dynamiclibrary.hpp>
#include <stout/lambda.hpp>
#include <stout/memory.hpp>

#include <mesos/mesos.hpp>

namespace mesos {

#define MODULE_API_VERSION "1"

#define MODULE_API_VERSION_FUNCTION moduleApiVersion

#define DEFINE_API_VERSION() \
  std::string MODULE_API_VERSION_FUNCTION() { return MODULE_API_VERSION; }

class ModuleInfo
{
public:
  int version;

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
  ModuleManager() : version(1)
  {

  }

  /**
   * Phase 1 and 2.
   */
  Try<Nothing> parseFlag(std::string flagValue);

  const int version;
private:
  // Phase 1.
  Try<DynamicLibrary&> loadLibrary(std::string path);

  // Used for phase 2b.
  Try<ModuleInfo> getModuleInfo(DynamicLibrary& lib, std::string moduleName);

  hashmap<std::string, Module> modules;
};


} // namespace mesos {

namespace module {

template <typename T>
Try<memory::shared_ptr<T> > init(
    DynamicLibrary& library,
    const std::string& entrySymbol,
    void* optionalArguments = NULL)
{
  Try<void*> symbol = library.loadSymbol(entrySymbol);
  if (symbol.isError()) {
    return Error(symbol.error());
  }

  if (symbol.get() == NULL) {
    return Error("Symbol should not be NULL pointer");
  }

  void* (*entryFunction)(void*) = (void *(*)(void*))symbol.get();
  void* moduleData = entryFunction(optionalArguments);

  // TODO(nnielsen): dynamic_cast does not work on void pointers.
  T* module = (T*)(moduleData);

  return memory::shared_ptr<T>(module);
}

}

// The module class works as a wrapper "around" the external implementation
// (brought in the dynamic loaded library).
class Module : public boost::noncopyable
{
public:
  virtual ~Module() { }

protected:
  const std::string mesosVersion_;
  int moduleSystemVersion_;
  int moduleVersion_;

  enum ModuleIdentifier {
    UNKNOWN_MODULE = 0,
    TEST_MODULE = 1,
    ISOLATOR_MODULE = 2
  } moduleIdentifier_;

  Module(
      ModuleIdentifier id,
      int moduleVersion = 1,
      int moduleSystemVersion = 1,
      const std::string mesosVersion = MESOS_VERSION)
    : mesosVersion_(MESOS_VERSION),
      moduleSystemVersion_(moduleSystemVersion),
      moduleIdentifier_(id) { }

};

#endif // __MODULE_HPP__
