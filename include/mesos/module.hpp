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

#define MESOS_VERSION_FUNCTION \
  mesos_get_version

#define MESOS_MODULE_API_VERSION_FUNCTION \
  mesos_get_module_api_version

#define MESOS_GET_MODULE_ROLE_ mesos_get_module_role_
#define MESOS_GET_MODULE_ROLE_FUNCTION(identifier) \
  MESOS_GET_MODULE_ROLE_##identifier

#define MESOS_CREATE_MODULE mesos_create_module_
#define MESOS_CREATE_MODULE_FUNCTION(identifier) \
  MESOS_CREATE_MODULE_##identifier

#define MESOS_IS_MODULE_COMPATIBILE_ mesos_is_module_compatible_
#define MESOS_IS_MODULE_COMPATIBILE_FUNCTION(identifier) \
  MESOS_IS_MODULE_COMPATIBILE_##identifier

#define MESOS_MODULE(role, identifier) \
  extern "C" const char* MESOS_VERSION_FUNCTION() { return MESOS_VERSION; } \
  extern "C" const char* MODULE_API_VERSION_FUNCTION() { \
    return MODULE_API_VERSION; \
  } \
  extern "C" const char* MESOS_GET_MODULE_ROLE_FUNCTION(identifier)() { \
    return #role; \
  } \
  extern "C" role* MESOS_CREATE_MODULE_FUNCTION(identifier)()

#define MESOS_MODULE_IS_COMPATIBILE(identifier) \
  extern "C" bool MESOS_MODULE_COMPATIBILITY_FUNCTION(identifier)()

#endif // __MODULE_HPP__
