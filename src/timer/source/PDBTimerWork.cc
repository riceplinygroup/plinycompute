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
#include "PDBTimerWork.h"
#include "DataTypes.h"
#include <iterator>
#include <stdlib.h>

using namespace std;
using namespace pdb;

pdb::PDBTimerWork::PDBTimerWork() {
    this->timerList = new list<PDBTimerPtr>();
    lastTime = (struct timeval){0};
    if (gettimeofday(&lastTime, nullptr) != 0) {
        exit(EXIT_FAILURE);
    }
    this->prevTicks = 0;
    this->runTimer = true;
    this->ticks = 0;
}

pdb::PDBTimerWork::PDBTimerWork(PDBWorkerQueuePtr workers, long long tickFrequency) {
    this->timerList = new list<PDBTimerPtr>();
    lastTime = (struct timeval){0};
    if (gettimeofday(&lastTime, nullptr) != 0) {
        exit(EXIT_FAILURE);
    }
    this->prevTicks = 0;
    this->runTimer = true;
    this->workerQueue = workers;
    this->tickFrequency = tickFrequency;
    this->ticks = 0;
}

pdb::PDBTimerWork::~PDBTimerWork() {
    delete this->timerList;
    this->timerList = nullptr;
}

void pdb::PDBTimerWork::addTimer(PDBTimerPtr timer) {

    if (this->timerList == nullptr) {
        exit(EXIT_FAILURE);
    }

    if (timer == nullptr) {
        return;
    }



    timer->setExpire(this->ticks + timer->getPeriodicTimeout());
    if (this->timerList->size() == 0) {
        this->timerList->push_back(timer);
    } else {
        PDBTimerPtr curTimer;
        list<PDBTimerPtr>::iterator timerIter;
        for (timerIter = this->timerList->begin(); timerIter != this->timerList->end(); timerIter++) {
            curTimer = *timerIter;
            if (curTimer->getExpire() >= timer->getExpire()) {
                //find a timer that expires later than this one
                //insert this timer before the first later timer found
                this->timerList->insert(timerIter, timer);
                break;
            }   
        }
    }
}

void  pdb::PDBTimerWork::updateTicks() {
    //this->getLogger()->writeLn("PDBTimerWork: update ticks...");
    struct timeval currentTime = (struct timeval){0};
    if (gettimeofday(&currentTime, nullptr) == 0) {
        ticks_t diffTicks = ((long long) currentTime.tv_sec * 1000000 + currentTime.tv_usec - ((long long) lastTime.tv_sec * 1000000 + lastTime.tv_usec)) * this->tickFrequency / (1000000);
        this->ticks += diffTicks;
        this->lastTime = currentTime;
    } else {
        exit(EXIT_FAILURE);
    }
}

void  pdb::PDBTimerWork::timerHandler() {
    this->getLogger()->writeLn("PDBTimerWork: running...");
    PDBTimerPtr curTimer;
    list<PDBTimerPtr>::iterator timerIter;
    while (this->runTimer) {
        this->updateTicks();
        for (timerIter = this->timerList->begin(); timerIter != this->timerList->end(); timerIter++) {
            curTimer = *timerIter;
            if (curTimer->getExpire() <= this->ticks) {
                //Timer expired
                PDBWorkerPtr worker = this->workerQueue->getWorker();
                PDBWorkPtr work = curTimer->getWorkToDo();
                PDBBuzzerPtr buzzer = work->getLinkedBuzzer();
                worker->execute(work, buzzer);
                buzzer->wait(); //shall we wait here
                this->updateTicks(); //if we do not need to wait on the buzzer, do we need to update ticks?
                if (curTimer->getType() == PeriodicTimer) {
                    this->addTimer(curTimer);
                } else {
                    this->timerList->erase(timerIter);
                }
            } else {
                break;
            }
        }
        usleep((long long) 1000000 / this->tickFrequency);
        //sleep (1);
    }

}

void  pdb::PDBTimerWork::execute(PDBBuzzerPtr callerBuzzer) {
    this->getLogger()->writeLn("PDBTimerWork: running...");
    this->timerHandler();
}

