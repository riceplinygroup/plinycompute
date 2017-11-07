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

#ifndef CAT_SHARED_LIB_BY_NAME_REQ_H
#define CAT_SHARED_LIB_BY_NAME_REQ_H

#include "Object.h"
#include "Handle.h"

// PRELOAD %CatSharedLibraryByNameRequest%

namespace pdb {

// encapsulates a request to obtain a shared library from the catalog
// given the name of the type, this is used when the typeId is unknown
class CatSharedLibraryByNameRequest : public Object {

public:
    CatSharedLibraryByNameRequest(){};

    CatSharedLibraryByNameRequest(int16_t identifierIn, String NameIn)
        : identifier(identifierIn), objectTypeName(NameIn) {}

    CatSharedLibraryByNameRequest(const CatSharedLibraryByNameRequest& objectToCopy) {
        objectTypeName = objectToCopy.objectTypeName;
        identifier = objectToCopy.identifier;
    }

    ~CatSharedLibraryByNameRequest(){};

    String getTypeLibraryName() {
        return objectTypeName;
    }

    int16_t getTypeLibraryId() {
        return identifier;
    }

    ENABLE_DEEP_COPY

private:
    int16_t identifier;
    String objectTypeName;
};
}

#endif
