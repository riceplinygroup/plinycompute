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

#ifndef GENERIC_WORK_H
#define GENERIC_WORK_H

#include "PDBWork.h"

// This template is used to make a simple piece of work with a particular execute function, and some
// state
namespace pdb {

class GenericWork : public PDBWork {

public:
    // this accepts the lambda that is used to process the RequestType object
    GenericWork(function<void(PDBBuzzerPtr)> executeMeIn) {
        executeMe = executeMeIn;
    }

    void execute(PDBBuzzerPtr callerBuzzer) {
        executeMe(callerBuzzer);
    }

private:
    function<void(PDBBuzzerPtr)> executeMe = nullptr;
};
}

#endif
