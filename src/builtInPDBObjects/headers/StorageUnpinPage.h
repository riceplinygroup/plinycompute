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

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_UNPINPAGE_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_UNPINPAGE_H_

#include "Object.h"
#include "DataTypes.h"

//  PRELOAD %StorageUnpinPage%

namespace pdb {
// this object type is sent to the server to tell it to unpin a page.
class StorageUnpinPage : public pdb::Object {


public:
    StorageUnpinPage() {}
    ~StorageUnpinPage() {}
    // get/set nodeId
    NodeID getNodeID() {
        return this->nodeId;
    }
    void setNodeID(NodeID nodeId) {
        this->nodeId = nodeId;
    }
    // get/set databaseId
    DatabaseID getDatabaseID() {
        return this->dbId;
    }
    void setDatabaseID(DatabaseID dbId) {
        this->dbId = dbId;
    }
    // get/set userTypeId
    UserTypeID getUserTypeID() {
        return this->userTypeId;
    }
    void setUserTypeID(UserTypeID typeId) {
        this->userTypeId = typeId;
    }
    // get/set setId
    SetID getSetID() {
        return this->setId;
    }
    void setSetID(SetID setId) {
        this->setId = setId;
    }
    // get/set pageId
    PageID getPageID() {
        return this->pageId;
    }
    void setPageID(PageID pageId) {
        this->pageId = pageId;
    }

    // get/set wasDirty
    bool getWasDirty() {
        return this->wasDirty;
    }
    void setWasDirty(bool wasDirty) {
        this->wasDirty = wasDirty;
    }

    ENABLE_DEEP_COPY

private:
    NodeID nodeId;
    DatabaseID dbId;
    UserTypeID userTypeId;
    SetID setId;
    PageID pageId;
    bool wasDirty;
};
}


#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_UNPINPAGE_H_ */
