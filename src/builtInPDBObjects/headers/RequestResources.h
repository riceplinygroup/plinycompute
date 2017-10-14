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

#ifndef REQUEST_RESOURCES_H
#define REQUEST_RESOURCES_H

#include "Object.h"
#include "Handle.h"

// PRELOAD %RequestResources%

namespace pdb {

// encapsulates a request to request resources for running a query
class RequestResources : public Object {

public:
    RequestResources() {}
    ~RequestResources() {}

    RequestResources(int numCores, int memSize) : numCores(numCores), memSize(memSize) {}

    int getNumCores() {
        return numCores;
    }

    int getMemSize() {
        return memSize;
    }

    ENABLE_DEEP_COPY

private:
    // number of CPU cores
    int numCores;

    // total size of memory (in MB)
    int memSize;
};
}

#endif
