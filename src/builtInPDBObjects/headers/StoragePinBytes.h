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

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_PINBYTES_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_PINBYTES_H_

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StoragePinBytes%

namespace pdb {
// this object type is sent to the server to tell it to pin a page
class StoragePinBytes : public pdb::Object {


public:
    StoragePinBytes() {}

    ~StoragePinBytes() {}

    StoragePinBytes(
        NodeID nodeId, DatabaseID dbId, UserTypeID userTypeId, SetID setId, size_t sizeOfBytes)
        : nodeId(nodeId),
          dbId(dbId),
          userTypeId(userTypeId),
          setId(setId),
          sizeOfBytes(sizeOfBytes) {}

    NodeID getNodeID() {
        return this->nodeId;
    }

    void setNodeID(NodeID nodeId) {
        this->nodeId = nodeId;
    }

    DatabaseID getDatabaseID() {
        return this->dbId;
    }

    void setDatabaseID(DatabaseID dbId) {
        this->dbId = dbId;
    }

    UserTypeID getUserTypeID() {
        return this->userTypeId;
    }

    void setUserTypeID(UserTypeID typeId) {
        this->userTypeId = typeId;
    }

    SetID getSetID() {
        return this->setId;
    }

    void setSetID(SetID setId) {
        this->setId = setId;
    }

    size_t getSizeOfBytes() {
        return this->sizeOfBytes;
    }

    void setSizeOfBytes(size_t sizeOfBytes) {
        this->sizeOfBytes = sizeOfBytes;
    }

    ENABLE_DEEP_COPY

private:
    NodeID nodeId;
    DatabaseID dbId;
    UserTypeID userTypeId;
    SetID setId;
    size_t sizeOfBytes;
};
}

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_PINBYTES_H_ */
