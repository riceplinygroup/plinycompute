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
#include "Reflection.h"

namespace pdb {

Reflection::Reflection() {
  this->isLoaded = false;
  this->sharedLibrary = nullptr;
}

bool Reflection::load(std::string fileName) {

  // load the debugging info
  auto didLoad = reader.load(fileName);

  // did we fail loading them
  if(!didLoad) {
    return false;
  }

  // load the .so
  this->sharedLibrary = dlopen(fileName.c_str(), RTLD_NOW);

  // did we succeed
  if(this->sharedLibrary != nullptr) {
    this->isLoaded = true;
  }

  // return the success or failure
  return this->isLoaded;
}

ClassReflectionPtr Reflection::getClassReflection(std::string className) {
  return std::make_shared<ClassReflection>(this->reader.getClassInformation(className), this->sharedLibrary);
}

ClassReflectionPtr Reflection::getClassReflection(const std::type_info &typeInfo) {
  return std::make_shared<ClassReflection>(this->reader.getClassInformation(typeInfo), this->sharedLibrary);
}

}

