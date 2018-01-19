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

#ifndef PDB_WORKER_Q_C
#define PDB_WORKER_Q_C

#include <iostream>
#include <climits>
#include "LockGuard.h"
#include "NothingWork.h"
#include <limits.h>
#include "PDBWorkerQueue.h"

namespace pdb {

extern void* stackBase;
extern void* stackEnd;

PDBWorkerQueue::PDBWorkerQueue(PDBLoggerPtr myLoggerIn, int numWorkers) {

    // first, make sure that another worker queue does not exist
    if (stackBase != nullptr) {
        std::cout << "I have detected that you have started two PDBWorkerQueue objects in this "
                     "process.\n";
        std::cout << "This is a really bad idea.  It probably will result in a crash at some "
                     "point, and\n";
        std::cout << "regardless of that, the idea is to have one queue that everyone draws "
                     "workers from.\n";
    }

    pthread_mutex_init(&waitingMutex, nullptr);
    pthread_mutex_init(&workingMutex, nullptr);
    pthread_cond_init(&waitingSignal, nullptr);
    shuttingDown = false;
    numOut = 0;

    if (stackBase != nullptr) {
        std::cout
            << "This is bad... it appears that you are creating more than one worker queue!\n";
        exit(1);
    }

    // this is the location where each worker is going to have their stack
    origStackBase = malloc(1024 * 1024 * 4 * (numWorkers + 1));

    // align it to 2^22... chop off the last 22 bits, and then add 2^22 to the address
    stackBase = (void*)(((((size_t)origStackBase) >> 22) << 22) + (1024 * 1024 * 4));
    stackEnd = ((char*)stackBase) + 1024 * 1024 * 4 * numWorkers;

    // create each worker...
    for (int i = 0; i < numWorkers; i++) {
        // put an allocator at the base of the stack for this worker... give him 64MB of RAM to work
        // with
        new (i * 1024 * 1024 * 4 + ((char*)stackBase))
            Allocator(PDBWorkerQueue::defaultAllocatorBlockSize);

        // now create the worker
        addAnotherWorker(i * 1024 * 1024 * 4 + sizeof(Allocator) + ((char*)stackBase),
                         (i + 1) * 1024 * 1024 * 4 + ((char*)stackBase));
    }
    myLogger = myLoggerIn;
}

PDBLoggerPtr PDBWorkerQueue::getLogger() {
    return myLogger;
}

PDBWorkerQueue::~PDBWorkerQueue() {

    // let everyone know we are shutting down
    shuttingDown = true;

    // keep getting threads and giving them nothing to do... once they complete
    // the nothing work, they will die since shuttingDown = true.  This goes
    // until we can't get any thrads because there are no more of them outstanding
    while (true) {

        // basically, we repeatedly ask for workers; this call will return
        // when either (a) there is a worker available, or (b) there are
        // no workers available but there are no workers in existence
        PDBWorkerPtr myWorker = getWorker();
        if (myWorker == nullptr)
            break;

        PDBWorkPtr nothing{make_shared<NothingWork>()};
        myWorker->execute(nothing, nullptr);
    }

    // join with all of the threads
    for (pthread_t thread : threads) {
        if (pthread_join(thread, nullptr) != 0) {
            cout << "Error joining with thread as the worker queue is shutting down!!\n";
            exit(-1);
        }
    }

    // kill all of the mutexes/signal vars
    pthread_mutex_destroy(&waitingMutex);
    pthread_mutex_destroy(&workingMutex);
    pthread_cond_destroy(&waitingSignal);

    // kill the allocators and destroy the stack space
    for (int i = 0; i < threads.size(); i++) {
        ((Allocator*)(i * 1024 * 1024 * 4 + ((char*)stackBase)))->~Allocator();
    }
    free(origStackBase);
}

PDBWorkerPtr PDBWorkerQueue::getWorker() {

    PDBWorkerPtr myWorker;

    {
        // make sure there is a worker
        const LockGuard guard{waitingMutex};
        while (waiting.size() == 0 && numOut != 0) {
            pthread_cond_wait(&waitingSignal, &waitingMutex);
        }

        // special case: there are no workers... just return a null
        if (numOut == 0) {
            return nullptr;
        }

        // get the worker
        myWorker = waiting.back();
        waiting.pop_back();
    }

    {
        // remember that he is working
        const LockGuard guard{workingMutex};
        working.insert(myWorker);
    }

    return myWorker;
}

// this is the entry point for all of the worker threads

void* enterTheQueue(void* pdbWorkerQueueInstance) {
    PDBWorkerQueue* temp = static_cast<PDBWorkerQueue*>(pdbWorkerQueueInstance);
    temp->enter();
    return nullptr;
}

void PDBWorkerQueue::addAnotherWorker(void* stackBaseIn, void* stackEndIn) {

    pthread_t thread;
    threads.push_back(thread);

    // adjust the stack base to align it correctly
    stackBaseIn = (void*)((((long)stackBaseIn + (PTHREAD_STACK_MIN - 1)) / PTHREAD_STACK_MIN) *
                          PTHREAD_STACK_MIN);

    // create a thread with an allocator located at the base of its stack
    pthread_attr_t tattr;
    pthread_attr_init(&tattr);
    // pthread_attr_setdetachstate(&tattr, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstack(&tattr, stackBaseIn, ((char*)stackEndIn) - (char*)stackBaseIn);

    int return_code = pthread_create(&(threads[threads.size() - 1]), &tattr, enterTheQueue, this);
    if (return_code) {
        cout << "ERROR; return code from pthread_create () is " << return_code << '\n';
        exit(-1);
    } else {
        numOut++;
    }
}

void PDBWorkerQueue::notifyAllWorkers(PDBAlarm withMe) {
    const LockGuard guard{workingMutex};

    // loop through all of the current workers
    for (PDBWorkerPtr p : working) {

        // sound the buzzer...
        p->soundBuzzer(withMe);
    }
}

void PDBWorkerQueue::enter() {

    // create the work
    PDBWorkerPtr temp{make_shared<PDBWorker>(this)};

    // then work until (a) someone told us that we need to die, or
    // (b) we are trying to shut down the worker queue
    while (!shuttingDown) {

        // put the work on the end of the queue
        {
            const LockGuard guard{waitingMutex};
            waiting.push_back(temp);
            pthread_cond_signal(&waitingSignal);
        }

        // and enter the work
        temp->enter();

        // when we have exited the work, it means that we are done, do we are no longer working
        {
            const LockGuard guard{workingMutex};
            if (working.erase(temp) != 1) {
                cout << "Why did I find != 1 copies of the worker?";
                exit(-1);
            }
        }

        // make sure to kill all references to stuff that we want to be able to dellocate
        // or else we will block and that stuff will sit around forever
        temp->reset();
    }

    // if we exited the loop, then this particular worker
    // needs to die... so decrement the count of outstanding
    // workers and let everyone know if the number is down to zero
    {
        // decrement the count of outstanding workers
        const LockGuard guard{waitingMutex};
        numOut--;

        // if there are no outstanding workers, everyone is woken up and gets a null
        if (numOut == 0)
            pthread_cond_broadcast(&waitingSignal);
    }
}
}

#endif
