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
#ifndef DATA_PACKET_H
#define DATA_PACKET_H

// by Jia, Aug 4th

#include <pthread.h>

class DataPacket {

private:
    // bytes
    void* bytes;

    // size
    size_t size;

    // mutex
    pthread_mutex_t mutex;


    // reference count
    int refCount;

public:
    // constructor
    DataPacket(void* bytes, size_t size) {
        this->bytes = bytes;
        this->size = size;
        this->refCount = 0;
        pthread_mutex_init(&mutex, nullptr);
    }

    // destructor
    ~DataPacket() {
        pthread_mutex_destroy(&mutex);
    }

    // get bytes
    void* getBytes() {
        return this->bytes;
    }

    // get size
    size_t getSize() {
        return this->size;
    }

    // get reference count
    int getRefCount() {
        return this->refCount;
    }

    // increment reference count
    void incRefCount() {
        pthread_mutex_lock(&mutex);
        this->refCount++;
        pthread_mutex_unlock(&mutex);
    }

    // decrement reference count
    void decRefCount() {
        pthread_mutex_lock(&mutex);
        this->refCount--;
        pthread_mutex_unlock(&mutex);
        if (this->refCount < 0) {
            std::cout << "WARNING: data packet reference count < 0" << std::endl;
        }
    }
};


#endif
