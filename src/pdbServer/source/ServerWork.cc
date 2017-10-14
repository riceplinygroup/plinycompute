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

#ifndef SERVER_WORK_CC
#define SERVER_WORK_CC

#include "ServerWork.h"

namespace pdb {

ServerWork::ServerWork(PDBServer& workOnMeIn) : workOnMe(workOnMeIn) {
    wasEnError = false;
}

PDBCommWorkPtr ServerWork::clone() {
    PDBCommWorkPtr returnVal{make_shared<ServerWork>(workOnMe)};
    return returnVal;
}

void ServerWork::handleError() {
    wasEnError = true;
    getLogger()->error("ServerWork: got an error");
}

PDBBuzzerPtr ServerWork::getLinkedBuzzer() {
    return std::make_shared<PDBBuzzer>([&](PDBAlarm myAlarm) {});
}

void ServerWork::execute(PDBBuzzerPtr callerBuzzer) {

    // while there is still something to do on this connection
    getLogger()->trace("ServerWork: about to handle a request");
    PDBBuzzerPtr myBuzzer{getLinkedBuzzer()};
    while (!wasEnError && workOnMe.handleOneRequest(myBuzzer, getCommunicator())) {
        getLogger()->trace("ServerWork: just handled another request");
        myBuzzer = getLinkedBuzzer();
    }

    getLogger()->trace("ServerWork: done with this server work");
    callerBuzzer->buzz(PDBAlarm::WorkAllDone);
}
}

#endif
