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

#ifndef BACKEND_TEST_SET_COPY_H
#define BACKEND_TEST_SET_COPY_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "DataTypes.h"
#include <string>

// PRELOAD %BackendTestSetCopy%

namespace pdb {

// encapsulates a request to copy a set stored in the database into another set
class BackendTestSetCopy : public Object {

public:
    BackendTestSetCopy(DatabaseID dbIdIn,
                       UserTypeID typeIdIn,
                       SetID setIdIn,
                       DatabaseID dbIdOut,
                       UserTypeID typeIdOut,
                       SetID setIdOut) {
        srcDatabaseID = dbIdIn;
        srcUserTypeID = typeIdIn;
        srcSetID = setIdIn;
        destDatabaseID = dbIdOut;
        destUserTypeID = typeIdOut;
        destSetID = setIdOut;
    }

    BackendTestSetCopy() {}
    ~BackendTestSetCopy() {}

    DatabaseID getDatabaseIn() {
        return srcDatabaseID;
    }

    UserTypeID getTypeIdIn() {
        return srcUserTypeID;
    }

    SetID getSetIdIn() {
        return srcSetID;
    }

    DatabaseID getDatabaseOut() {
        return destDatabaseID;
    }

    UserTypeID getTypeIdOut() {
        return destUserTypeID;
    }

    SetID getSetIdOut() {
        return destSetID;
    }

    ENABLE_DEEP_COPY

private:
    // this is the database ID of the source set
    DatabaseID srcDatabaseID;

    // this is the type ID of the source set
    UserTypeID srcUserTypeID;

    // this is the ID of the source set
    SetID srcSetID;

    // this is the database ID of the destination set
    DatabaseID destDatabaseID;

    // this is the type ID of the destination set
    UserTypeID destUserTypeID;

    // this is the ID of the destination set
    SetID destSetID;
};
}

#endif
