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
 * File:   PDBTimer.h
 * Author: Jia 
 *
 * Created on October 19, 2015, 9:30 PM
 */

#ifndef PDBTIMER_H
#define	PDBTIMER_H

#include <memory>
using namespace std;

#include "DataTypes.h"
#include "PDBWork.h"

/**
 * This class implements a Timer, which is associated with:
 * - ticks_t timeout: the timeout interval in ticks;
 * - PDBWorkPtr workToDo: the work to be executed when the timer expires;
 * - TimerType type: is it a one-shot timer or periodic timer.
 */

namespace pdb {

class PDBTimer;
typedef shared_ptr<PDBTimer> PDBTimerPtr;

class PDBTimer {

public:

	/**
	 * Create a timer instance.
	 */
    PDBTimer(ticks_t timeout, PDBWorkPtr workToDo, TimerType type) {
        this->setPeriodicTimeout(timeout);
        this->setWorkToDo(workToDo);
        this->setType(type);
    }

    ~PDBTimer() {
    }

    /**
     * return the expire time
     */
    ticks_t getExpire() const {
        return expire;
    }

    /**
     * return the timeout interval
     */
    ticks_t getPeriodicTimeout() const {
        return periodicTimeout;
    }

    /**
     * return the work to be executed when the timer expires
     */
    PDBWorkPtr getWorkToDo() const {
        return workToDo;
    }

    /**
     * return the type of the timer
     */
    TimerType getType() const {
        return type;
    }

    /**
     * set the expire time for the timer
     */
    void setExpire(ticks_t expire) {
        this->expire = expire;
    }

    /**
     * set the timeout interval for the timer
     */
    void setPeriodicTimeout(ticks_t periodicTimeout) {
        this->periodicTimeout = periodicTimeout;
    }

    /**
     * set the work to be executed when timer expires
     */
    void setWorkToDo(PDBWorkPtr workToDo) {
        this->workToDo = workToDo;
    }

    /**
     * set the type of this timer: PeriodicTimer or OneShotTimer
     */
    void setType(TimerType type) {
        this->type = type;
    }

private:
    ticks_t expire; //expire time
    ticks_t periodicTimeout; //timer length
    PDBWorkPtr workToDo;
    TimerType type;







};
}

#endif	/* PDBTIMER_H */

