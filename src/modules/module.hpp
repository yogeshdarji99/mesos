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

// module::instantiate<>(library, symbol, <arg>)

namespace module {

template <typename T>
Try<memory::shared_ptr<T> > init(
    DynamicLibrary& library,
    const std::string& entrySymbol,
    void* optionalArguments = NULL)
{
  lambda::function<memory::shared_ptr<T>(void*)> entryFunction;

  Try<void*> symbol = library.loadSymbol(entrySymbol);
  if (symbol.isError()) {
    return Error(symbol.error());
  }

  if (symbol.get() == NULL) {
    return Error("Symbol should not be NULL pointer");
  }

  entryFunction =
    lambda::bind(
        reinterpret_cast<memory::shared_ptr<T>(*)(void*)>(symbol.get()),
        lambda::_1);
  return entryFunction(optionalArguments);
}

}

class Module : public boost::noncopyable {
public:
  Module(int version = 1) : version_(version) { }

  virtual ~Module() { }

  virtual int version() { return version_; }

protected:
  int version_;
};

#endif // __MODULE_HPP__
