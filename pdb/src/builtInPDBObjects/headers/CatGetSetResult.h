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

#ifndef CATGETSETBASERESULT_H
#define CATGETSETBASERESULT_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatGetSetResult%

namespace pdb {

/**
 * Encapsulates a request to search for a type in the catalog
 */
class CatGetSetResult : public Object {

 public:

  CatGetSetResult() = default;
  ~CatGetSetResult() = default;

  /**
   * Creates a request to get the database
   * @param database - the name of database
   */
  explicit CatGetSetResult(const std::string &database,
                           const std::string &set,
                           const std::string &internalType,
                           const std::string &type) : databaseName(database),
                                                      setName(set),
                                                      internalType(internalType),
                                                      type(type) {}

  ENABLE_DEEP_COPY

  /**
   * The name of the database
   */
  String databaseName;

  /**
   * The name of the set
   */
  String setName;

  /**
   * The the name of the internal type that is going to be handling the types.
   * For example a pdb::Vector<StringIntPair> is going to be handled by pdb::Vector<pdb::Nothing>
   */
  String internalType;

  /**
   * The real name of the type see above
   */
  String type;
};
}

#endif
