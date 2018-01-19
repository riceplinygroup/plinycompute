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

#ifndef SHAREDMEM_H
#define SHAREDMEM_H

#include <pthread.h>
#include "PDBLogger.h"
#include "SlabAllocator.h"

#ifndef USE_MEMCACHED_SLAB_ALLOCATOR
#include "tlsf.h"
#endif

#include <memory>
using namespace std;
class SharedMem;
typedef shared_ptr<SharedMem> SharedMemPtr;


//this class wraps a shared memory buffer pool for allocating pages
//this class uses mmap system call

class SharedMem {
public:
    SharedMem(size_t shmMemSize, pdb::PDBLoggerPtr logger);
    ~SharedMem();
    void lock();
    void unlock();
    void* malloc(size_t size);
    void* mallocAlign(size_t size, size_t alignment, int& offset);
    void free(void* ptr, size_t size);
    long long computeOffset(void* shmAddress);
    void* getPointer(size_t offset);
    static char* addressRoundUp(char* address, size_t roundTo);
    static size_t roundUp(size_t size, size_t roundTo);
    static size_t roundDown(size_t size, size_t roundTo);
    void* _malloc_unsafe(size_t size);
    void _free_unsafe(void* ptr, size_t size);
    size_t getShmSize();

protected:
    int initialize();
    void destroy();
    int getMem();
    int initMallocs();
    int initMutex();

private:
    pthread_mutex_t* memLock;
    pdb::PDBLoggerPtr logger;
#ifdef USE_MEMCACHED_SLAB_ALLOCATOR
    SlabAllocatorPtr allocator;
#else
    tlsfAllocator allocator;
    void* my_tlsf;
#endif
    void* memPool;
    size_t shmMemSize;
};

#endif /* SHAREDMEM_H */
