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

#ifndef CATGETDATABASERESULT_H
#define CATGETDATABASERESULT_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatGetDatabaseResult%

namespace pdb {

/**
 * Encapsulates a request to search for a type in the catalog
 */
class CatGetDatabaseResult : public Object {

 public:

  CatGetDatabaseResult() = default;
  ~CatGetDatabaseResult() = default;

  /**
   * Creates a request to get the database via
   * @param database - the name of database
   */
  explicit CatGetDatabaseResult(const std::string &database, long createdOn) : database(database), createdOn(createdOn) {}

  ENABLE_DEEP_COPY

  /**
   * The name of the database
   */
  String database;

  /**
   * The timestamp this database is created on
   */
  long createdOn;
};
}

#endif
