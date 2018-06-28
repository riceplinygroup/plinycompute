/*****************************************************************************
 *                                                                           *
 *  Copyright 2018 Rice University                                           *
 *                                                                           *
 *  Licensed under the Apache License, Version 2.0 (the "License");          *
 *  you may not use this file except in compliance with the License.         *
 *  You may obtain a copy of the License at                                  *
 *                                                                           *
 *      http://www.apache.org/licenses/LICENSE-2.0                           *
 *                                                                           *
 *  Unless required by applicable law or agreed to in writing, software      *
 *  distributed under the License is distributed on an "AS IS" BASIS,        *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. *
 *  See the License for the specific language governing permissions and      *
 *  limitations under the License.                                           *
 *                                                                           *
 *****************************************************************************/

#ifndef TESTINGCLASSSTUFF_METHODREFLECTION_H
#define TESTINGCLASSSTUFF_METHODREFLECTION_H

#include "SymbolReader.h"

namespace pdb {

class MethodReflection;
typedef std::shared_ptr<MethodReflection> MethodReflectionPtr;

class MethodReflection {

public:

  MethodReflection(std::string methodName, const std::vector<methodInfo> &possibleMethods, void *sharedLibrary);

  /**
   * Put a pointer of a particular type
   * @param address - the address
   * @param type - the type of the pointer
   */
  void putPointer(void *address, std::string type);

  /**
   * Put an integer value
   * @param value - the value
   */
  void putInt(int value);

  /**
   * The character value
   * @param value - the value
   */
  void putChar(char value);

  bool execute();

 protected:

  /**
   * The possible methods
   */
  std::vector<methodInfo> possibleMethods;

  /**
   * The shared library we loaded
   */
  void *sharedLibrary;

  /**
   * The name of this method
   */
  std::string methodName;

  /**
   * The stack buffer
   */
  std::vector<int8_t> stackBuffer;

  /**
   * The list of types stored on the stack
   */
  std::vector<std::string> stackTypes;
};

}

#endif //TESTINGCLASSSTUFF_METHODREFLECTION_H
