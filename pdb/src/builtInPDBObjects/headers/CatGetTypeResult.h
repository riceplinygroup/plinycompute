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

#ifndef CAT_TYPE_SEARCH_RES_H
#define CAT_TYPE_SEARCH_RES_H

#include "Object.h"
#include "Handle.h"

// PRELOAD %CatGetTypeResult%

namespace pdb {

/**
 * Stores the result of searching for a type
 */
class CatGetTypeResult : public Object {

 public:

  CatGetTypeResult() = default;
  ~CatGetTypeResult() = default;

  CatGetTypeResult(int16_t searchRes, const std::string &typeName, const std::string &typeCategory) : typeID(searchRes),
                                                                                                      typeName(typeName),
                                                                                                      typeCategory(typeCategory) {}
  ENABLE_DEEP_COPY

  /**
   * The type id of the type
   */
  int16_t typeID = -1;

  /**
   * The name of the type
   */
  pdb::String typeName;

  /**
   * The category of the type
   */
  pdb::String typeCategory;

};
}

#endif
