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
#include "SharedMem.h"
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <err.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <string>
#include <iostream>


#define FRAG_END(address) ((fragEnd_t *)((char*)(address) + sizeof(frag_t) + (address)->size))
#define FRAG_NEXT(address) ((frag_t *)((char *)(address) + sizeof(frag_t) + (address)->size + sizeof(fragEnd_t)))
#define FRAG_PREV(address) ((frag_t *)(((char *)(address) - sizeof(fragEnd_t)) - \
    ((fragEnd_t *)((char*)(address) - sizeof(fragEnd_t)))->size - sizeof(frag_t)))
#define PREV_FRAG_END(address) ((fragEnd *)((char *)(address) - sizeof(fragEnd_t)))
#define FRAG_OVERHEAD (sizeof(frag_t) + sizeof(fragEnd_t))

SharedMem::SharedMem(size_t memSize, pdb :: PDBLoggerPtr logger) {
    this->shmMemSize = memSize;
//    cout << "shared memory size = "<<memSize<<"\n";
    this->memPool = nullptr;
    if (this->getMem() < 0) {
        std :: cout << "Fatal error: initialize shared memory failed with size="
<< memSize << std :: endl;
        logger->error(std :: string("Fatal error: initialize shared memory failed with size=") + std :: to_string(memSize));
        exit(-1);
    } 
    this->allocator = make_shared<ScopedAllocator>(this->memPool, this->shmMemSize);
    this->initMutex();
    this->logger = logger;
}

SharedMem::~SharedMem() {
    this->destroy();
}

void SharedMem::lock() {
    pthread_mutex_lock(this->memLock);
}

void SharedMem::unlock() {
    pthread_mutex_unlock(this->memLock);
}

void* SharedMem::malloc(size_t size) {
    void * ptr;
    //this->logger->writeLn("SharedMem: executing malloc()...");
    //this->logger->writeInt(size);
    this->lock();
    //this->logger->writeLn("SharedMem: got lock.");
    ptr = this->allocator->_malloc_unsafe(size);
    this->unlock();
    return ptr;
}


void * SharedMem::mallocAlign (size_t size, size_t alignment, int& offset) {
    void * ptr = this->malloc(size + alignment);
    void * retPtr = ptr;
    offset = 0;
    if(ptr != nullptr) { 
        //ptr = (void *)((size_t)((char*)ptr + 511) & ~ (size_t)(0x01FF));
        retPtr = (void *)this->addressRoundUp((char *)ptr, alignment);
        offset = (char *)retPtr - (char *)ptr;
    }
    return retPtr;
}


void SharedMem::free(void *ptr) {
    this->lock();
    this->allocator->_free_unsafe(ptr);
    this->unlock();
}


char* SharedMem::addressRoundUp(char* address, size_t roundTo) {
    size_t roundToMask = (~((size_t) roundTo - 1));
    return (char *)((size_t)((address) + (roundTo - 1)) & roundToMask);
}

size_t SharedMem::roundUp(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t) roundTo - 1));
    return (((address) + (roundTo - 1)) & roundToMask);
}

size_t SharedMem::roundDown(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t) roundTo - 1));
    return ((address) & roundToMask);
}


int SharedMem::initialize() {
    return 0;
}

void SharedMem::destroy() {
    if (this->memLock) {
        //this->logger->writeLn("SharedMem: destroying the shared memory lock.");
        pthread_mutex_destroy(this->memLock);
    }
    if (this->memPool && (this->memPool != (void*) - 1)) {
        munmap(this->memPool, this->shmMemSize);
        this->memPool = (void*) - 1;
    }
}

int SharedMem::getMem() {
    if (this->memPool && (this->memPool != (void*) - 1)) {
        //this->logger->writeLn("SharedMem: shared memory already initialized.");
        return -1;
    }
    this->memPool = mmap(0, this->shmMemSize, PROT_READ | PROT_WRITE,
            MAP_ANON | MAP_SHARED, -1, 0);
    if (this->memPool == (void*) - 1) {
        //this->logger->writeLn("SharedMem: could not attach shared memory segment:");
        //this->logger->writeLn(strerror(errno));
        return -1;
    }
    //this->logger->writeLn("SharedMem: get memory successfully.");
    return 0;
}

long long SharedMem::computeOffset(void* shmAddress) {
    return (long long) ((char*) shmAddress - (char*) this->memPool);
}

void * SharedMem::getPointer(size_t offset) {
    return (void *) ((size_t)this->memPool + (size_t) offset);
}

int SharedMem::initMutex() {
    this->memLock = (pthread_mutex_t *)this->_malloc_unsafe(sizeof (pthread_mutex_t));
    if (this->memLock == 0) {
        //this->logger->writeLn("SharedMem: could not allocate lock.");
        return -1;
    }
    if (pthread_mutex_init(this->memLock, nullptr) != 0) {
        //this->logger->writeLn("SharedMem: could not initialize lock.");
        return -1;
    }
    //this->logger->writeLn("SharedMem: initiate lock successfully.");
    return 0;
}

void* SharedMem::_malloc_unsafe(size_t size) {
    return this->allocator->_malloc_unsafe(size);
}

void SharedMem::_free_unsafe(void * ptr) {
    return this->allocator->_free_unsafe(ptr);
}

