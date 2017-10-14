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
 * File:   PDBWork.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:19 PM
 */

#ifndef PDBWORK_H
#define PDBWORK_H


#include <memory>
using namespace std;

#include "PDBBuzzer.h"
#include "PDBLogger.h"
#include "PDBWorker.h"
#include "PDBWorkerQueue.h"
#include <pthread.h>
#include <string>

// this class wraps up a bit of work that needs to be accomplished...
// the idea is that one first obtains a PDBWorker that has been loaded
// with this work; they call PDBWorker.execute () which executes the
// work, and then they get the result.

namespace pdb {

// create a smart pointer for PDBWork objects
class PDBWork;
typedef shared_ptr<PDBWork> PDBWorkPtr;

class PDBWork {
public:
    // actually go off and do the work... the worker can optionally invoke
    // any of the handlers embedded in callerBuzzer to ask the caller to
    // deal with errors
    virtual void execute(PDBBuzzerPtr callerBuzzer) = 0;

    // gets a buzzer that is "linked" to this object; that is, a PDBBuzzer
    // object that (optionally) overrides PDBBuzzer.handleBuzz () and performs
    // a callback to this object, invoking specialized code that handles the
    // event.  For example, we might create a buzzer object with a PDBBuzzer.handleBuzz ()
    // that catches any PDBAlarm (other than PDBAlarm :: WorkAllDone) and then
    // calls a method on this object to handle the  event
    virtual PDBBuzzerPtr getLinkedBuzzer();

    // this is called by the PDBWorker class to initiate the work
    void execute(PDBWorkerQueue* parent, PDBBuzzerPtr callerBuzzer);

    // gets another worker from the PDBWorkQueue that was used to
    // create the PDBWorker who is running this guy...
    PDBWorkerPtr getWorker();

    // gets access to the logger
    PDBLoggerPtr getLogger();

private:
    // this is the work queue that this dude came from... used to supply
    // additional workers, if they are requested
    PDBWorkerQueue* parent;
};
}

#endif /* PDBWORK_H */
