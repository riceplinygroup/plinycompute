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
 * File:   PDBWorker.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:11 PM
 */

#ifndef PDBWORKER_H
#define PDBWORKER_H

// create a smart pointer for PDBWorker objects
#include <memory>
#include "PDBAlarm.h"
#include "PDBBuzzer.h"
#include "PDBWork.h"
#include "PDBWorkerQueue.h"

// this class wraps up a thread... note that PDBWorker objects should always be created
// by the PDBWorkerQueue class.

namespace pdb {

class PDBWorker;
typedef shared_ptr<PDBWorker> PDBWorkerPtr;

class PDBWork;
typedef shared_ptr<PDBWork> PDBWorkPtr;

class PDBWorker {
public:
    // constructor... accepts the work queue that is creating it
    PDBWorker(PDBWorkerQueue* parent);

    // destructor
    ~PDBWorker();

    // gets another worder from the same work queue that this guy came from... note that
    // this call can block if the work queue does not have any additional workers
    PDBWorkerPtr getWorker();

    // asks this worker to execute runMe... this call is non-blocking.  When the task
    // is done, myBuzzer->buzz () should be called.
    void execute(PDBWorkPtr runMe, PDBBuzzerPtr myBuzzer);

    // directly sound the buzzer on this guy to wake him if he is sleeping
    void soundBuzzer(PDBAlarm withMe);

    // gets access to the logger
    PDBLoggerPtr getLogger();

    // entry point for a thread
    void enter();

    // this resets the contents
    void reset();

private:
    // the work queue we came from
    PDBWorkerQueue* parent;

    // the work to run
    PDBWorkPtr runMe;

    // used to signal when we are done, or when some event happens
    PDBBuzzerPtr buzzWhenDone;

    // for coordinating the thread who is running this guy
    pthread_mutex_t workerMutex;
    pthread_cond_t workToDoSignal;

    // set to true when the worker is able to go and do the work
    bool okToExecute;
};
}

#endif /* PDBWORKER_H */
