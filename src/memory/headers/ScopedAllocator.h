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
 * File: ScopedAllocator.h
 * Author: Jia
 *
 * Created on May 17, 2016, 16:30
 */

#ifndef SCOPEDALLOCATOR_H
#define SCOPEDALLOCATOR_H

#include <memory>
using namespace std;
class ScopedAllocator;
typedef shared_ptr<ScopedAllocator> ScopedAllocatorPtr;

#define MIN_FRAG_SIZE  (size_t)(8)
//#define MEDIUM_FRAG_SIZE (size_t)(512*1024)
#define OPTIMIZE_FACTOR (size_t)(10)
#define OPTIMIZE_SIZE (size_t)(1 << OPTIMIZE_FACTOR)
#define MAX_FACTOR (size_t) (36)
#define MAX_FRAG_SIZE (size_t)(1 << MAX_FACTOR)  //this value should be equivalent with page size
#define HASH_SIZE ((OPTIMIZE_SIZE)/MIN_FRAG_SIZE + MAX_FACTOR-OPTIMIZE_FACTOR)
//#define HASH_SIZE ((OPTIMIZE_SIZE)/MIN_FRAG_SIZE +(MAX_FRAG_SIZE-OPTIMIZE_SIZE)/MEDIUM_FRAG_SIZE+1)

struct frag_t {
    size_t size;
    union {
        frag_t * next_free;
        long is_free;
    } u;
};

struct fragEnd_t {
//    size_t size;
    frag_t * prev_free;
};

struct fragLink_t {
    frag_t head;
    fragEnd_t tail;
    int numFrags;
};

struct block_t {
    size_t size;
    size_t used;
    size_t real_used;
    size_t max_real_used;
    size_t freeFrags;
    frag_t * firstFrag;
    fragEnd_t * lastFragEnd;
    fragLink_t freeHash[HASH_SIZE];
};

class ScopedAllocator {
public:
    ScopedAllocator(void * mempool, size_t size);
    ~ScopedAllocator();
    static size_t roundUp(size_t size, size_t roundTo);
    static size_t roundDown(size_t size, size_t roundTo);
    static int getHash(size_t size);
    static int getBigHash(size_t size);
    void * _malloc_unsafe(size_t size);
    void _free_unsafe(void *ptr);

protected:
    void insert_free(frag_t * frag);
    void detach_free(frag_t * frag);
    frag_t * find_free(size_t size, int* hash);
    int split_frag(frag_t * frag, size_t newSize);
    int initMalloc();
private:
    void * memPool;
    size_t memSize;
    block_t * block;

};


#endif
