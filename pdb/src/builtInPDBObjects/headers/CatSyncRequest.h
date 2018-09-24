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

#ifndef CATALOG_NODE_METADATA_H_
#define CATALOG_NODE_METADATA_H_

#include <iostream>
#include "Object.h"
#include "PDBString.h"
#include "PDBVector.h"

//  PRELOAD %CatSyncRequest%

using namespace std;

namespace pdb {

/**
 * This class is used to sync a worker node with the manager
 */
class CatSyncRequest : public Object {
 public:

  CatSyncRequest() = default;

  CatSyncRequest(const std::string &nodeIP, int port, const std::string &nodeType) {

    // init the fields
    this->nodeIP = nodeIP;
    this->nodePort = port;
    this->nodeType = nodeType;
  }

  explicit CatSyncRequest(const Handle<CatSyncRequest> &requestToCopy) {
    nodeIP = requestToCopy->nodeIP;
    nodePort = requestToCopy->nodePort;
    nodeType = requestToCopy->nodeType;
  }

  ~CatSyncRequest() = default;

  ENABLE_DEEP_COPY

  /**
   * IP address of the node
   */
  pdb::String nodeIP;

  /**
   * The port of the node
   */
  int nodePort = -1;

  /**
   * The type of the node "worker" or "manager"
   */
  pdb::String nodeType;
};

} /* namespace pdb */

#endif /* CATALOG_NODE_METADATA_H_ */
