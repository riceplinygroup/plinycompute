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

#include "ClassReflection.h"

namespace pdb {

ClassReflection::ClassReflection(const classInfo &clsInfo, void *sharedLibrary) : sharedLibrary(sharedLibrary),
                                                                                  className(clsInfo.className),
                                                                                  attributes(clsInfo.attributes) {
  // go through each method
  for(auto &method : *clsInfo.methods) {

    // if we don't have the vector add it
    this->methods.try_emplace(method.name, std::vector<methodInfo>());

    // add the methods
    this->methods[method.name].push_back(method);
  }
}

MethodReflectionPtr ClassReflection::getMethod(std::string methodName) {

  if(this->methods.find(methodName) == this->methods.end()) {
    return nullptr;
  }

  return std::make_shared<MethodReflection>(methodName, methods[methodName], this->sharedLibrary);
}

}


