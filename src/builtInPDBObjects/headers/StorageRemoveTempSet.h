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

#ifndef STORAGE_REMOVE_TEMP_SET_H
#define STORAGE_REMOVE_TEMP_SET_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "DataTypes.h"

// PRELOAD %StorageRemoveTempSet%

namespace pdb {

// encapsulates a request to remove a temp set from storage
class StorageRemoveTempSet : public Object {

public:
    StorageRemoveTempSet() {}
    ~StorageRemoveTempSet() {}

    StorageRemoveTempSet(SetID setId) : setId(setId) {}

    SetID getSetID() {
        return setId;
    }


    ENABLE_DEEP_COPY

private:
    SetID setId;
};
}

#endif
