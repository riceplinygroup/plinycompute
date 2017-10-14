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

#ifndef PDB_BUZZ_C
#define PDB_BUZZ_C

#include "PDBBuzzer.h"
#include <iostream>


PDBBuzzer::PDBBuzzer() {

    pthread_cond_init(&waitingSignal, nullptr);
    pthread_mutex_init(&waitingMutex, nullptr);
    stringFunc = nullptr;
    signalSent = false;
}

PDBBuzzer::PDBBuzzer(std::nullptr_t) {
    pthread_cond_init(&waitingSignal, nullptr);
    pthread_mutex_init(&waitingMutex, nullptr);
    stringFunc = nullptr;
    signalSent = false;
}


void PDBBuzzer::buzz(PDBAlarm withMe) {
    pthread_mutex_lock(&waitingMutex);
    if (noStringFunc != nullptr)
        noStringFunc(withMe);
    pthread_cond_signal(&waitingSignal);
    signalSent = true;
    pthread_mutex_unlock(&waitingMutex);
}

void PDBBuzzer::buzz(PDBAlarm withMe, string message) {
    pthread_mutex_lock(&waitingMutex);
    if (stringFunc != nullptr)
        stringFunc(withMe, message);
    pthread_cond_signal(&waitingSignal);
    signalSent = true;
    pthread_mutex_unlock(&waitingMutex);
}

void PDBBuzzer::buzz(PDBAlarm withMe, int& counter) {
    pthread_mutex_lock(&waitingMutex);
    if (intFunc != nullptr)
        intFunc(withMe, counter);
    pthread_cond_signal(&waitingSignal);
    signalSent = true;
    pthread_mutex_unlock(&waitingMutex);
}

void PDBBuzzer::wait() {

    // wait until there is a buzz
    pthread_mutex_lock(&waitingMutex);
    if (signalSent == true) {
        pthread_mutex_unlock(&waitingMutex);
        return;
    }
    pthread_cond_wait(&waitingSignal, &waitingMutex);
    pthread_mutex_unlock(&waitingMutex);
}

PDBBuzzer::PDBBuzzer(std::function<void(PDBAlarm)> noStringFuncIn) {
    pthread_cond_init(&waitingSignal, nullptr);
    pthread_mutex_init(&waitingMutex, nullptr);
    noStringFunc = noStringFuncIn;
    signalSent = false;
}

PDBBuzzer::PDBBuzzer(std::function<void(PDBAlarm, std::string)> stringFuncIn) {
    pthread_cond_init(&waitingSignal, nullptr);
    pthread_mutex_init(&waitingMutex, nullptr);
    stringFunc = stringFuncIn;
    signalSent = false;
}

PDBBuzzer::PDBBuzzer(std::function<void(PDBAlarm, int&)> intFuncIn) {
    pthread_cond_init(&waitingSignal, nullptr);
    pthread_mutex_init(&waitingMutex, nullptr);
    intFunc = intFuncIn;
    signalSent = false;
}


PDBBuzzer::~PDBBuzzer() {
    pthread_cond_destroy(&waitingSignal);
    pthread_mutex_destroy(&waitingMutex);
}


#endif
