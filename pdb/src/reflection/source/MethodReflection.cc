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

#include <dlfcn.h>
#include "MethodReflection.h"

namespace pdb {

MethodReflection::MethodReflection(std::string methodName,
                                   const std::vector<MethodInfo> &possibleMethods,
                                   void *sharedLibrary) : possibleMethods(possibleMethods),
                                                          sharedLibrary(sharedLibrary),
                                                          methodName(methodName){}

void MethodReflection::putPointer(void *address, std::string type) {

  // add the type
  stackTypes.emplace_back(type + "*");

  // convert the thing a char pointer
  auto addressValues = (char*) &address;

  // insert the thing to the stack
  stackBuffer.insert(stackBuffer.end(), addressValues, addressValues + sizeof(void*));
}

void MethodReflection::putInt(int value) {

  // add the type
  stackTypes.emplace_back("int");

  // convert the thing a char pointer
  auto addressValues = (char*) &value;

  // insert the thing to the stack
  stackBuffer.insert(stackBuffer.end(), addressValues, addressValues + sizeof(int));
}

void MethodReflection::putChar(char value) {

  // add the type
  stackTypes.emplace_back("char");

  // convert the thing a char pointer
  auto addressValues = (char*) &value;

  // insert the thing to the stack
  stackBuffer.insert(stackBuffer.end(), addressValues, addressValues + sizeof(char));
}

bool MethodReflection::execute() {

  // grab the method
  auto methodSymbol = possibleMethods.begin()->symbol;
  auto function = (void(*)(void*, int, char)) dlsym(this->sharedLibrary, methodSymbol.c_str());

  char buffer[1000];
  int64_t n = 1;

  //asm("subq $0xD, %rsp");

  //asm ("callq *%0"
  //     : "=r" (function));

  function(&buffer, 10, 'c');

  return true;
}

}

