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
/*
 * File:   ServerWork.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:05 PM
 */

#ifndef SERVERWORK_H
#define SERVERWORK_H


#include "PDBBuzzer.h"
#include "PDBCommWork.h"
#include "PDBServer.h"

namespace pdb {

#include <memory>
class ServerWork;
typedef shared_ptr<ServerWork> ServerWorkPtr;

// does all of the work associated with one connection to the server

class ServerWork : public PDBCommWork {
public:
    // specify the server we are working on
    ServerWork(PDBServer& workOnMe);

    // do the actual work
    void execute(PDBBuzzerPtr callerBuzzer) override;

    // error handler
    void handleError();

    // clone this guy
    PDBCommWorkPtr clone() override;

    // get buzzer linked to this guy
    PDBBuzzerPtr getLinkedBuzzer() override;

private:
    // calling handleError sets this to true
    bool wasEnError;

    // the server we are doing the work for
    PDBServer& workOnMe;
};
}

#endif /* SERVERWORK_H */
