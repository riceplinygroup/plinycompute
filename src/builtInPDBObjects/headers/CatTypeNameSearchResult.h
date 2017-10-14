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

#ifndef CAT_TYPE_SEARCH_REQ_H
#define CAT_TYPE_SEARCH_REQ_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"

// PRELOAD %CatTypeNameSearchResult%

namespace pdb {

// encapsulates a request to obtain a type name from the catalog
class CatTypeNameSearchResult : public Object {

public:
    CatTypeNameSearchResult(){};
    ~CatTypeNameSearchResult(){};
    CatTypeNameSearchResult(std::string typeName, bool success, std::string errMsg)
        : typeName(typeName), errMsg(errMsg), success(success) {}

    std::string getTypeName() {
        return typeName;
    }

    std::pair<bool, std::string> wasSuccessful() {
        return std::make_pair(success, errMsg);
    }

    ENABLE_DEEP_COPY

private:
    String typeName;
    String errMsg;
    bool success;
};
}

#endif
