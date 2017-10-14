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

#ifndef PDB_WORKER_C
#define PDB_WORKER_C

#include "Allocator.h"
#include "LockGuard.h"
#include "PDBWorker.h"
#include <iostream>

namespace pdb {

PDBWorker::PDBWorker(PDBWorkerQueue* parentIn) : parent(parentIn) {
    pthread_mutex_init(&workerMutex, nullptr);
    pthread_cond_init(&workToDoSignal, nullptr);
    okToExecute = false;
}

PDBWorkerPtr PDBWorker::getWorker() {
    return parent->getWorker();
}

PDBLoggerPtr PDBWorker::getLogger() {
    return parent->getLogger();
}

void PDBWorker::execute(PDBWorkPtr runMeIn, PDBBuzzerPtr buzzWhenDoneIn) {
    const LockGuard guard{workerMutex};
    runMe = runMeIn;
    buzzWhenDone = buzzWhenDoneIn;
    okToExecute = true;
    pthread_cond_signal(&workToDoSignal);
}

void PDBWorker::soundBuzzer(PDBAlarm withMe) {
    if (buzzWhenDone != nullptr) {
        buzzWhenDone->buzz(withMe);
    }
}

void PDBWorker::enter() {

    // wait for someone to tell us it is OK to do work
    {
        const LockGuard guard{workerMutex};
        while (okToExecute == false) {
            pthread_cond_wait(&workToDoSignal, &workerMutex);
        }
    }
    getAllocator().cleanInactiveBlocks((size_t)(67108844));
    getAllocator().cleanInactiveBlocks((size_t)(12582912));
    // then do the work
    runMe->execute(parent, buzzWhenDone);
    okToExecute = false;
}

void PDBWorker::reset() {
    runMe = nullptr;
    buzzWhenDone = nullptr;
}

PDBWorker::~PDBWorker() {
    pthread_mutex_destroy(&workerMutex);
    pthread_cond_destroy(&workToDoSignal);
}
}

#endif
