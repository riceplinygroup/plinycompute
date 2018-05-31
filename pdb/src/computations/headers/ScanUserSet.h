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

#ifndef SCAN_USER_SET_H
#define SCAN_USER_SET_H

// PRELOAD %ScanUserSet <Nothing>%

#include "ScanUserSetBase.h"

namespace pdb {

/**
 * to scan a user set
 * @tparam OutputClass
 */
template<class OutputClass>
class ScanUserSet : public ScanUserSetBase<OutputClass> {

public:

  /**
 * This constructor is for constructing builtin object
 */
  ScanUserSet() = default;

  /**
   * The following should be used to crate the ScanUserSet
   * @param dbName
   * @param setName
   */
  ScanUserSet(std::string dbName, std::string setName) {
    this->dbName = dbName;
    this->setName = setName;
    this->outputType = getTypeName<OutputClass>();
    this->batchSize = -1;
  }

};
}

#endif
