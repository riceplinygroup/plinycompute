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

#ifndef CAT_SHARED_LIB_RES_H
#define CAT_SHARED_LIB_RES_H

#include "Object.h"
#include "Handle.h"
#include "PDBVector.h"
#include <utility>

// PRELOAD %CatSharedLibraryResult%

namespace pdb {

// encapsulates a shared library set from the catalog
class CatSharedLibraryResult : public Object {

public:
    CatSharedLibraryResult() {}
    CatSharedLibraryResult(bool res, std::string errMsg) : res(res), errMsg(errMsg) {}
    ~CatSharedLibraryResult() {}

    // this is used to actually store the shared library
    // will be loaded when sendBytes () is called...
    Vector<char> dataToSend;

    ENABLE_DEEP_COPY

    std::pair<bool, std::string> getRes() {
        return std::make_pair(res, errMsg);
    }

private:
    bool res;
    String errMsg;
};
}

#endif
