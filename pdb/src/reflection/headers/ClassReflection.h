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

#ifndef TESTINGCLASSSTUFF_CLASSREFLECTION_H
#define TESTINGCLASSSTUFF_CLASSREFLECTION_H

#include <memory>
#include <map>
#include "SymbolReader.h"
#include "MethodReflection.h"


namespace pdb {

class ClassReflection;
typedef std::shared_ptr<ClassReflection> ClassReflectionPtr;

class ClassReflection {
 public:

  /**
   * Creates a new reflection class
   * @param clsInfo
   * @param sharedLibrary
   */
  ClassReflection(const ClassInfo &clsInfo, void *sharedLibrary);

  /**
   * Grab the method with the name
   * @param methodName - the method name
   * @return the reflected method
   */
  MethodReflectionPtr getMethod(std::string methodName);

 private:

  /**
   * The class name with the full namespace specification
   */
  std::string className;

  /**
   * The attributes of the class
   */
  std::shared_ptr<std::vector<AttributeInfo>> attributes;

  /**
   * Maps the name of the method to the
   */
  std::map<std::string, std::vector<MethodInfo>> methods;

  /**
   * The shared library we loaded
   */
  void *sharedLibrary;
};

}

#endif //TESTINGCLASSSTUFF_CLASSREFLECTION_H
