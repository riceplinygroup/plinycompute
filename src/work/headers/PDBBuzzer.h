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
 * File:   PDBBuzzer.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:10 PM
 */

#ifndef PDBBUZZER_H
#define	PDBBUZZER_H

#include <memory>
using namespace std;

// create a smart pointer for PDBBuzzer objects
class PDBBuzzer;
typedef shared_ptr <PDBBuzzer> PDBBuzzerPtr;

// the way that this works is that anyone who is waiting on the buzzer (via a call to wait) is
// blocked until someone calls buzz.  After the call to buzz:
// 
// (1) first, the appropriate handleBuzz function is called (either the one with the message or 
//	without the message)
// (2) second, anyone blocked on wait is woken up
//
// The way that this is used is that when one has work to do, they will create a piece of work,
// as well as a worker, and then get the buzzer associated with that worker.  They then ask the
// worker to do the work, and (optionally) they block on the work.  When the work finishes, it
// will typically call buzz (PDBAlarm :: WorkAllDone), causing anyone blocked on the work to 
// wake up.  

#include "PDBAlarm.h"
#include <pthread.h>
#include <string>
#include <functional>

class PDBBuzzer {
public:

    // sounds the buzzer, causing noStringFunc () to be called
    void buzz(PDBAlarm withMe);

    // sounds the buzzer, causing stringFunc () to be called
    void buzz(PDBAlarm withMe, string message);

    // blocks until someone calls buzz 
    void wait();

    // constructor, destructor
    PDBBuzzer(std::function <void (PDBAlarm)>);
    PDBBuzzer(std::function <void (PDBAlarm, string)>);
    ~PDBBuzzer();

private:

    pthread_mutex_t waitingMutex;
    pthread_cond_t waitingSignal;
    bool signalSent = false;
    std::function <void (PDBAlarm)> noStringFunc = nullptr; 
    std::function <void (PDBAlarm, std::string)> stringFunc = nullptr; 
};



#endif	/* PDBBUZZER_H */

