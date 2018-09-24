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

#ifndef CAT_CREATE_SET_H
#define CAT_CREATE_SET_H

#include "Object.h"
#include "PDBString.h"
#include "Handle.h"

// PRELOAD %CatCreateSetRequest%

namespace pdb {

// encapsulates a request to create a set
class CatCreateSetRequest : public Object {

 public:
  ~CatCreateSetRequest() = default;
  CatCreateSetRequest() = default;

  CatCreateSetRequest(const std::string &dbName,
                      const std::string &setName,
                      const std::string &typeName,
                      int16_t typeID) : dbName(dbName), setName(setName), typeName(typeName), typeID(typeID) {}

  CatCreateSetRequest(const Handle<CatCreateSetRequest> &requestToCopy) {
    dbName = requestToCopy->dbName;
    setName = requestToCopy->setName;
    typeName = requestToCopy->typeName;
    typeID = requestToCopy->typeID;
  }

  ENABLE_DEEP_COPY

  /**
   * The name of the database
   */
  String dbName;

  /**
   * The name of the set
   */
  String setName;

  /**
   * The name of the type
   */
  String typeName;

  /**
   * The type id
   */
  int16_t typeID = -1;
};

}

#endif
