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

#ifndef PDB_SERVER_TEMP_CC
#define PDB_SERVER_TEMP_CC

#include "Handle.h"
#include "PDBServer.h"
#include "ServerFunctionality.h"
#include <memory>

namespace pdb {

template <class Functionality, class... Args>
void PDBServer::addFunctionality(Args&&... args) {

    // first, get the name of this type
    std::string myType = getTypeName<Functionality>();

    // and remember him... map him to a particular index in the list of functionalities
    if (allFunctionalityNames.count(myType) == 1) {
        std::cerr << "BAD!  You can't add the same functionality twice.\n";
    }
    allFunctionalityNames[myType] = allFunctionalities.size();

    // then create the functionality
    shared_ptr<ServerFunctionality> whichFunctionality = make_shared<Functionality>(args...);
    allFunctionalities.push_back(whichFunctionality);

    registerHandlersFromLastFunctionality();
}

template <class Functionality>
Functionality& PDBServer::getFunctionality() {

    // first, figure out which index we are
    static int whichIndex = -1;
    if (whichIndex == -1) {
        std::string myType = getTypeName<Functionality>();
        whichIndex = allFunctionalityNames[myType];
    }

    // and now, return the functionality
    return *((Functionality*)allFunctionalities[whichIndex].get());
}
}

#endif
