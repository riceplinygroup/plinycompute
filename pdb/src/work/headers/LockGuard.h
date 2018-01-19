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
 * File:   LockGuard.h
 * Author: Chris
 *
 * Created on September 25, 2015, 5:12 PM
 */

#ifndef LOCKGUARD_H
#define LOCKGUARD_H


#include <pthread.h>
#include <iostream>
#include <sstream>

// simple RAII-style helper to hold a lock for the duration of its own
// lifetime, typically determined by some scoped block; similar to
// std::lock_guard, but for pthread_mutex_t rather than std::mutex

class LockGuard {
public:
    // lock on construction
    explicit LockGuard(pthread_mutex_t& mutex) : mutex(mutex) {
        // std :: stringstream ss;
        // ss << &mutex;
        // std :: cout << "to get lock at " << ss.str() << std :: endl;
        pthread_mutex_lock(&mutex);
        // std :: cout << "got lock at " << ss.str() << std :: endl;
    }

    // unlock on destruction

    ~LockGuard() {
        // std :: stringstream ss;
        // ss << &mutex;
        // std :: cout << "to unlock at " << ss.str() << std :: endl;
        pthread_mutex_unlock(&mutex);
        // std :: cout << "unlocked at " << ss.str() << std :: endl;
    }

    // forbidden, to avoid double-unlock bugs
    LockGuard(const LockGuard&) = delete;
    LockGuard& operator=(const LockGuard&) = delete;

private:
    // underlying system mutex to manage
    pthread_mutex_t& mutex;
};


#endif /* LOCKGUARD_H */
