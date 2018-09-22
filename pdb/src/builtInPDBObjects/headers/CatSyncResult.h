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
/*
 * CatSyncRequest.h
 *
 */

#ifndef CATALOG_SYNC_RESULT_H_
#define CATALOG_SYNC_RESULT_H_

#include <iostream>
#include <vector>
#include <memory>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"


//  PRELOAD %CatSyncResult%

using namespace std;

namespace pdb {

/**
 * This class is used to sync a worker node with the manager
 */
class CatSyncResult : public Object {
public:

  CatSyncResult() = default;

  explicit CatSyncResult(const std::vector<unsigned char> &bytes) {

    // init the fields
    this->bytes = pdb::makeObject<pdb::Vector<unsigned char>>(bytes.size(), bytes.size());

    // copy the bytes
    memcpy(this->bytes->c_ptr(), bytes.data(), bytes.size());
  }

  CatSyncResult(const Handle<CatSyncResult> &requestToCopy)  {
    // init the fields
    this->bytes = pdb::makeObject<pdb::Vector<unsigned char>>(requestToCopy->bytes->size(), requestToCopy->bytes->size());

    // copy the bytes
    memcpy(bytes->c_ptr(), requestToCopy->bytes->c_ptr(), bytes->size());
  }

  ~CatSyncResult() = default;

  ENABLE_DEEP_COPY

  /**
   * The bytes we want to send
   */
  pdb::Handle<Vector<unsigned char>> bytes;
};

} /* namespace pdb */

#endif /* CATALOG_NODE_METADATA_H_ */
