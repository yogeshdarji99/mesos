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

#include "modules/module.hpp"
#include "modules/isolator_module.hpp"
#include "module.hpp"

#include "tests/flags.hpp"
#include "tests/mesos.hpp"

using namespace mesos;
using namespace mesos::internal;
using namespace mesos::internal::tests;

class ModulesTest : public MesosTest {};


TEST_F(ModulesTest, LoadModuleTest)
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
          ) + ":TestModule");
  ASSERT_SOME(result);

  Try<memory::shared_ptr<TestModule> > module =
    manager.createTestModule();
  ASSERT_SOME(module);

  EXPECT_EQ(module.get()->foo('a', 1024), 0xabab);
  EXPECT_EQ(module.get()->bar(0.5, 0.75), 0xf0f0);
}


TEST_F(ModulesTest, LoadModuleTest)
{
  DynamicLibrary library;
  Try<Nothing> result = library.open(
      path::join(
          tests::flags.build_dir, "src", ".libs",
#ifdef __linux__
          "libtest.so"
#else
          "libtest.dylib"
#endif
          ));
  ASSERT_SOME(result);

  Try<memory::shared_ptr<TestModule> > module =
    TestModule::init(library);
  ASSERT_SOME(module);

  EXPECT_EQ(module.get()->foo('a', 1024), 0xabab);
  EXPECT_EQ(module.get()->bar(0.5, 0.75), 0xf0f0);
}

TEST_F(ModulesTest, IsolatorModuleTest)
{
  DynamicLibrary library;
  Try<Nothing> result = library.open(
      path::join(
          tests::flags.build_dir, "src", ".libs",
#ifdef __linux__
          "libtestisolator.so"
#else
          "libtestisolator.dylib"
#endif
          ));
  ASSERT_SOME(result);

  Try<memory::shared_ptr<mesos::internal::slave::Isolator> > module =
    mesos::internal::slave::IsolatorModule::init(library);
  ASSERT_SOME(module);
}

// TODO(nnielsen): Negative-test of module load of modules with
// in-correct type, API version, etc.
