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

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_GETSETPAGES_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_GETSETPAGES_H_

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StorageGetSetPages%

namespace pdb {
// this object type is sent to the server to tell it to load all pages in a set
class StorageGetSetPages : public pdb::Object {


public:
    StorageGetSetPages() {}
    ~StorageGetSetPages() {}
    // get/set node Id
    NodeID getNodeID() {
        return this->nodeId;
    }
    void setNodeID(NodeID nodeId) {
        this->nodeId = nodeId;
    }

    // get/set database Id
    DatabaseID getDatabaseID() {
        return this->dbId;
    }
    void setDatabaseID(DatabaseID dbId) {
        this->dbId = dbId;
    }

    // get/set userType Id
    UserTypeID getUserTypeID() {
        return this->userTypeId;
    }
    void setUserTypeID(UserTypeID typeId) {
        this->userTypeId = typeId;
    }

    // get/set set Id
    SetID getSetID() {
        return this->setId;
    }
    void setSetID(SetID setId) {
        this->setId = setId;
    }

    ENABLE_DEEP_COPY


private:
    NodeID nodeId;
    DatabaseID dbId;
    UserTypeID userTypeId;
    SetID setId;
};
}


#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_GETSETPAGES_H_ */
