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
#include "Configuration.h"
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

#ifndef USE_MEMCACHED_SLAB_ALLOCATOR
#include "tlsf.h"
#endif

SharedMem::SharedMem(size_t memSize, pdb::PDBLoggerPtr logger) {
    this->shmMemSize = memSize;
    this->memPool = nullptr;
    if (this->getMem() < 0) {
        std::cout << "Fatal error: initialize shared memory failed with size=" << memSize
                  << std::endl;
        logger->error(std::string("Fatal error: initialize shared memory failed with size=") +
                      std::to_string(memSize));
        exit(-1);
    }
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    this->allocator =
        make_shared<SlabAllocator>(this->memPool, this->shmMemSize, DEFAULT_PAGE_SIZE, 512);
#else
    this->my_tlsf = this->allocator.tlsf_create_with_pool(this->memPool, this->shmMemSize);
#endif
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

size_t SharedMem::getShmSize() {
    return this->shmMemSize;
}


void* SharedMem::malloc(size_t size) {
    void* ptr;
    this->lock();
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    ptr = this->allocator->slabs_alloc_unsafe(size);
#else
    ptr = this->allocator.tlsf_malloc(this->my_tlsf, size);
#endif
    this->unlock();
    return ptr;
}


void* SharedMem::mallocAlign(size_t size, size_t alignment, int& offset) {
    void* ptr = this->malloc(size + alignment);
    void* retPtr = ptr;
    offset = 0;
    if (ptr != nullptr) {
        retPtr = (void*)this->addressRoundUp((char*)ptr, alignment);
        offset = (char*)retPtr - (char*)ptr;
    }
    return retPtr;
}


void SharedMem::free(void* ptr, size_t size) {
    this->lock();
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    this->allocator->slabs_free_unsafe(ptr, size);
#else
    this->allocator.tlsf_free(this->my_tlsf, ptr);
#endif
    this->unlock();
}


char* SharedMem::addressRoundUp(char* address, size_t roundTo) {
    size_t roundToMask = (~((size_t)roundTo - 1));
    return (char*)((size_t)((address) + (roundTo - 1)) & roundToMask);
}

size_t SharedMem::roundUp(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t)roundTo - 1));
    return (((address) + (roundTo - 1)) & roundToMask);
}

size_t SharedMem::roundDown(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t)roundTo - 1));
    return ((address)&roundToMask);
}


int SharedMem::initialize() {
    return 0;
}

void SharedMem::destroy() {
    if (this->memLock) {
        pthread_mutex_destroy(this->memLock);
    }
    if (this->memPool && (this->memPool != (void*)-1)) {
        munmap(this->memPool, this->shmMemSize);
        this->memPool = (void*)-1;
    }
}

int SharedMem::getMem() {
    if (this->memPool && (this->memPool != (void*)-1)) {
        return -1;
    }
    this->memPool = mmap(0, this->shmMemSize, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, -1, 0);
    if (this->memPool == (void*)-1) {
        return -1;
    }
    return 0;
}

long long SharedMem::computeOffset(void* shmAddress) {
    return (long long)((char*)shmAddress - (char*)this->memPool);
}

void* SharedMem::getPointer(size_t offset) {
    return (void*)((size_t)this->memPool + (size_t)offset);
}

int SharedMem::initMutex() {
    this->memLock = (pthread_mutex_t*)this->_malloc_unsafe(sizeof(pthread_mutex_t));
    if (this->memLock == 0) {
        std::cout << "FATAL ERROR: can't allocate for memLock from buffer pool" << std::endl;
        return -1;
    }
    if (pthread_mutex_init(this->memLock, nullptr) != 0) {
        return -1;
    }
    return 0;
}

void* SharedMem::_malloc_unsafe(size_t size) {
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    return this->allocator->slabs_alloc_unsafe(size);
#else
    return this->allocator.tlsf_malloc(this->my_tlsf, size);
#endif
}

void SharedMem::_free_unsafe(void* ptr, size_t size) {
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    return this->allocator->slabs_free_unsafe(ptr, size);
#else
    return this->allocator.tlsf_free(this->my_tlsf, ptr);
#endif
}
