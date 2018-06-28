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

#ifndef TESTINGCLASSSTUFF_REFLECTION_H
#define TESTINGCLASSSTUFF_REFLECTION_H

#include "SymbolReader.h"
#include "ClassReflection.h"

namespace pdb {

class Reflection {
 public:

  /**
   * The constructor for the reflection
   */
  explicit Reflection();

  /**
   * Loads the specified file
   * @param fileName - the path to the .so library
   * @return true if we can load the library
   */
  bool load(std::string fileName);

  /**
   *
   * @param className
   * @return
   */
  ClassReflectionPtr getClassReflection(std::string className);

  /**
   *
   * @param typeInfo
   * @return
   */
  ClassReflectionPtr getClassReflection(const std::type_info &typeInfo);

 protected:

  /**
   * This thing reads the symbols
   */
  SymbolReader reader;

  /**
   * The shared library we loaded
   */
  void *sharedLibrary;

  /**
   * Is the reflection loaded
   */
  bool isLoaded;

  /**
   * The file name of the .so library
   */
  std::string fileName;
};

}


#endif //TESTINGCLASSSTUFF_REFLECTION_H
