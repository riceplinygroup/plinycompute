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

// A buzzer is an object given by someone who wants work done by a worker.  
//
// The buzzer's constructor takes as input a function to handle the buzzer being signaled.
//
// Typically, someone who wants work done will create a buzzer, and then give it to a bunch
// of workers, then will call wait () to sleep until work is completed.
//
// When a piece of work is completed, the woker then calls buzz (), which invokes the 
// function stored inside of the buzzer.
//
// One is guarnateed that each call to buzz () will result in a separate invocation of
// the function.

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

    // sounds the buzzer, causing workFunc () to be called
    void buzz(PDBAlarm withMe, int& counter);

    // blocks until someone calls buzz 
    void wait();

    // constructor, destructor
    PDBBuzzer();
    PDBBuzzer(std::nullptr_t nullp);
    PDBBuzzer(std::function <void (PDBAlarm)>);
    PDBBuzzer(std::function <void (PDBAlarm, string)>);
    PDBBuzzer(std::function <void (PDBAlarm, int&)>);
    ~PDBBuzzer();


private:

    pthread_mutex_t waitingMutex;
    pthread_cond_t waitingSignal;
    bool signalSent = false;
    std::function <void (PDBAlarm)> noStringFunc = nullptr; 
    std::function <void (PDBAlarm, std::string)> stringFunc = nullptr; 
    std::function <void (PDBAlarm, int&)> intFunc = nullptr;

};



#endif	/* PDBBUZZER_H */

