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

#ifndef EXEC_COMPUTATION_H
#define EXEC_COMPUTATION_H

#include "Object.h"
#include "PDBString.h"

// PRELOAD %ExecuteComputation%

namespace pdb {

// encapsulates a request to run a query
class ExecuteComputation : public Object {

public:
    ExecuteComputation() {}
    ~ExecuteComputation() {}

    ExecuteComputation(std::string tcapString) {
        this->tcapString = tcapString;
    }

    void setTCAPString(std::string tcapString) {
        this->tcapString = tcapString;
    }

    std::string getTCAPString() {
        return tcapString;
    }

    ENABLE_DEEP_COPY

private:
    String tcapString;
};
}

#endif
