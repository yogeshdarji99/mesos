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

#include <slave/containerizer/isolator.hpp>

#include <mesos/module.hpp>

#include "module.hpp"

#include "tests/flags.hpp"
#include "tests/mesos.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::tests;

class ModuleTest : public MesosTest {};

TEST_F(ModuleTest, ExampleModuleTest)
{
  ModuleManager manager;
  Try<Nothing> result = manager.loadLibraries(
      path::join(
          tests::flags.build_dir, "src", ".libs",
#ifdef __linux__
          "libtest.so"
#else
          "libtest.dylib"
#endif
          ) + ":example");
  EXPECT_SOME(result);

  Try<TestModule*> module =
    manager.loadModule<TestModule>("example");
  EXPECT_SOME(module);

  EXPECT_EQ(module.get()->foo('A', 1024), 1089);
  EXPECT_EQ(module.get()->bar(0.5, 10.8), 5);
}

TEST_F(ModuleTest, UnknownLibraryTest)
{
  ModuleManager manager;
  Try<Nothing> result = manager.loadLibraries(
      path::join(
          tests::flags.build_dir, "src", ".libs",
#ifdef __linux__
          "libunknown.so"
#else
          "libunknown.dylib"
#endif
          ) + ":example");
  EXPECT_ERROR(result);
}

TEST_F(ModuleTest, UnknownModuleTest)
{
  ModuleManager manager;
  Try<Nothing> result = manager.loadLibraries(
      path::join(
          tests::flags.build_dir, "src", ".libs",
#ifdef __linux__
          "libtest.so"
#else
          "libtest.dylib"
#endif
          ) + ":unknown");
  EXPECT_ERROR(result);
}

