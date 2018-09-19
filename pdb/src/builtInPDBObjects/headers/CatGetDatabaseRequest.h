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

#ifndef CATGETDATABASEREQUEST_H
#define CATGETDATABASEREQUEST_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatGetDatabaseRequest%

namespace pdb {

/**
 * Encapsulates a request to search for a type in the catalog
 */
class CatGetDatabaseRequest : public Object {

 public:

  CatGetDatabaseRequest() = default;
  ~CatGetDatabaseRequest() = default;

  /**
   * Creates a request to get the database
   * @param database - the name of database
   */
  explicit CatGetDatabaseRequest(const std::string &database) : databaseName(database) {}

  ENABLE_DEEP_COPY

  /**
   * The name of the database
   */
  String databaseName;
};
}

#endif
