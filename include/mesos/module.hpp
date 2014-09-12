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

#include <string>
#include <mesos/mesos.hpp>

// Module implementation utilities:

#define MODULE_API_VERSION "1"

#define MESOS_VERSION_FUNCTION mesos_get_version
#define MESOS_MODULE_API_VERSION_FUNCTION mesos_get_module_api_version

#define MESOS_GET_MODULE_ROLE_FUNCTION(name) mesos_get_role_##name
#define MESOS_CREATE_MODULE_FUNCTION(name) mesos_create_module_##name

#define MESOS_MODULE(role, name) \
  extern "C" const char* MESOS_VERSION_FUNCTION() { return MESOS_VERSION; } \
  extern "C" const char* MODULE_API_VERSION_FUNCTION() { \
    return MODULE_API_VERSION; \
  } \
  extern "C" const char* MESOS_GET_MODULE_ROLE_FUNCTION(name) () { \
    return #role; \
  } \
  extern "C" role* MESOS_CREATE_MODULE_FUNCTION(name) ()

#endif // __MODULE_HPP__
