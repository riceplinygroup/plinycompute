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
 * File: ScopedAllocator.cc
 * Author: Jia
 */

#include "ScopedAllocator.h"
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
//#define FRAG_NEXT(address) ((frag_t *)((char *)(address) + sizeof(frag_t) + (address)->size + sizeof(fragEnd_t)))
//#define FRAG_PREV(address) ((frag_t *)(((char *)(address) - sizeof(fragEnd_t)) - \
 //   ((fragEnd_t *)((char*)(address) - sizeof(fragEnd_t)))->size - sizeof(frag_t)))
//#define PREV_FRAG_END(address) ((fragEnd *)((char *)(address) - sizeof(fragEnd_t)))
#define FRAG_OVERHEAD (sizeof(frag_t) + sizeof(fragEnd_t))

ScopedAllocator::ScopedAllocator(void * memPool, size_t memSize) {
    this->memSize = memSize;
    this->memPool = memPool;
    //cout << "shared memory pool size="<<memSize<<"\n";
    //cout << "frag_t size="<< sizeof(frag_t) <<"\n";
    //cout << "size_t size="<< sizeof(size_t) <<"\n";
    //cout << "long size="<< sizeof(long) <<"\n";
    //cout << "void * size="<< sizeof(void *) << "\n";
    //cout << "hash size="<< HASH_SIZE <<"\n";
    //cout << "unsigned int size="<< sizeof(unsigned int)<<"\n";
    //cout << "int size="<< sizeof(int)<<"\n";
    //cout << "FRAG_OVERHEAD="<<FRAG_OVERHEAD<<"\n";
    if(this->initMalloc()<0) {
        exit(-1);
    }
}

ScopedAllocator::~ScopedAllocator() {
    if (this->block) {
        this->block = 0;
    }
}

size_t ScopedAllocator::roundUp(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t) roundTo - 1));
    return (((address) + (roundTo - 1)) & roundToMask);
}

size_t ScopedAllocator::roundDown(size_t address, size_t roundTo) {
    size_t roundToMask = (~((size_t) roundTo - 1));
    return ((address) & roundToMask);
}

/* size must be multiple of MIN_FRAG_SIZE*/
int ScopedAllocator::getHash(size_t size) {
    int index;
    if (size <= (OPTIMIZE_SIZE)) {
        return size / MIN_FRAG_SIZE;
    }
    //return (OPTIMIZE_SIZE) /MIN_FRAG_SIZE + size/MEDIUM_FRAG_SIZE + 1;
    
    index = getBigHash(size);
    return (OPTIMIZE_SIZE) / MIN_FRAG_SIZE + index - OPTIMIZE_FACTOR + 1;
    
}

int ScopedAllocator::getBigHash(size_t size) {
    int index;
    index = MAX_FACTOR;
    for (; !(size & ((size_t) 1 << (MAX_FACTOR))); size = size << 1, index--);
    return index;
}

void* ScopedAllocator::_malloc_unsafe(size_t size) {
    size = this->roundUp(size, MIN_FRAG_SIZE);
    //cout << "malloc size:"<<size<<"\n";
    //cout << "this->block->size="<<this->block->size<<"\n";
    //cout << "this->block->real_used="<<this->block->real_used<<"\n";
    if (size > (this->block->size - this->block->real_used)) {
        //cout << "ScopedAllocator: Out of Memory for allocating size="<<size<<"\n";
        //logger->writeLn("SharedMem: Out of Memory.");
        //logger->writeInt(size);
        //logger->writeInt(this->block->size);
        //logger->writeInt(this->block->real_used);
        return nullptr;
    }
    
    //this->logger->writeLn("SharedMem: there is enough memory.");
    struct frag_t* f;
    int hash;
    if ((f = this->find_free(size, &hash)) != 0) {
        this->detach_free(f);
        //mark it as busy
        f->u.is_free = 0;
        this->block->freeHash[hash].numFrags--;
        this->block->freeFrags--;
        split_frag(f, size);
        this->block->real_used += f->size;
        this->block->used += f->size;
        if (this->block->max_real_used < this->block->real_used) {
            this->block->max_real_used = this->block->real_used;
        }
        return (char*) f + sizeof (frag_t);
    }
    return nullptr;
}

void ScopedAllocator::_free_unsafe(void * ptr) {
    frag_t * f;
    size_t size;
    if (ptr == nullptr) {
        //logger->writeLn("SharedMem: free(0) called.");
        return;
    }
    f = (frag_t *) ((char *) ptr - sizeof (frag_t));
    if (f->u.is_free) {
        //logger->writeLn("SharedMem: freeing a free fragment.");
        cout <<"Error: freeing a free fragment!\n";
        return;
    }
    size = f->size;
    //cout << "size to free="<<size<<"\n";
    this->block->used -= size;
    this->block->real_used -= size;
    //this->logger->writeLn("SharedMem: invoking insert_free().");
    this->insert_free(f);
    //this->logger->writeLn("SharedMem: insert_free() done.");
    //cout <<"block->real_used after free="<<this->block->real_used<<"\n";
}

void ScopedAllocator::insert_free(frag_t * frag) {
    int hash = this->getHash(frag->size);
    //cout << "to free hash="<<hash<<"\n";
    frag_t * f;
    for (f = this->block->freeHash[hash].head.u.next_free;
            f != &(this->block->freeHash[hash].head); f = f->u.next_free) {
        if (frag->size <= f->size) {
            break;
        }
    }
    /* insert it here */
    frag_t* prev = FRAG_END(f)->prev_free;
    prev->u.next_free = frag;
    FRAG_END(frag)->prev_free = prev;
    frag->u.next_free = f;
    FRAG_END(f)->prev_free = frag;
    this->block->freeHash[hash].numFrags++;
    this->block->freeFrags++;
}

void ScopedAllocator::detach_free(frag_t * frag) {
    frag_t * prev;
    prev = FRAG_END(frag)->prev_free;
    frag_t * next;
    next = frag->u.next_free;
    prev->u.next_free = next;
    FRAG_END(next)->prev_free = prev;
}

frag_t * ScopedAllocator::find_free(size_t size, int * h) {
    unsigned int hash;
    frag_t * f;
    //cout << "size="<<size<<"\n";
    //cout << "hash="<<this->getHash(size)<<"\n"; 
    for (hash = this->getHash(size); hash < HASH_SIZE; hash++) {
        for (f = this->block->freeHash[hash].head.u.next_free; f != &(this->block->freeHash[hash].head);
                f = f->u.next_free) {
            if (f->size >= size) {
                *h = hash;
                //cout<<"hit hash="<<hash<<"\n";
                return f;
            }
        }
    }
    /* not found*/
    return nullptr;

}

int ScopedAllocator::split_frag(frag_t * frag, size_t newSize) {
    size_t rest;
    frag_t * f;
    fragEnd_t * end;
    rest = frag->size - newSize;
    if ((rest > (FRAG_OVERHEAD + (OPTIMIZE_SIZE))) ||
            (rest >= (FRAG_OVERHEAD + newSize))) {
        frag->size = newSize;
        end = FRAG_END(frag);
        //end->size = newSize;
        f = (frag_t*) ((char*) end + sizeof (fragEnd_t));
        f->size = rest - FRAG_OVERHEAD;
        //FRAG_END(f)->size = f->size;
        this->block->real_used += FRAG_OVERHEAD;
        this->insert_free(f);
        //cout << "block->real_used after split:"<<this->block->real_used<<"\n";
        return 0;
    } else {
        return -1;
    }
}

int ScopedAllocator::initMalloc() {
    char * start = (char *) this->roundUp((size_t) (this->memPool), MIN_FRAG_SIZE);
    if (this->memSize < (size_t)(start - (char *) this->memPool)) {
        //logger->writeLn("SharedMem: Out of Memory after roundUp size.");
        //logger->writeInt(this->shmMemSize);
        return -1;
    }
    size_t size = this->roundDown(this->memSize, MIN_FRAG_SIZE);
    size -= (start - (char *) this->memPool);
    size = this->roundDown(size, MIN_FRAG_SIZE);
    size_t initOverhead = this->roundUp(sizeof (block_t) + sizeof (frag_t) + sizeof (fragEnd_t), MIN_FRAG_SIZE);
    //cout << "initOverhead="<<initOverhead<<"\n";
    if (size < initOverhead) {
        //logger->writeLn("SharedMem: Out of Memory after roundUp initOverhead.");
        //logger->writeInt(size);
        //logger->writeInt(initOverhead);
        return -1;
    }
    char * end = start + size;
    this->block = (block_t *) start;
    memset(this->block, 0, sizeof (block_t));
    this->block->freeFrags = 0;
    this->block->size = size;
    this->block->real_used = initOverhead;
    this->block->max_real_used = this->block->real_used;
    size -= initOverhead;
    //logger->writeInt(size);
    this->block->firstFrag = (frag_t *) (start + this->roundUp(sizeof (block_t), MIN_FRAG_SIZE));
    this->block->firstFrag->size = size;
    this->block->lastFragEnd = (fragEnd_t *) (end - sizeof (fragEnd_t));
    //this->block->lastFragEnd->size = size;

    unsigned int h;
    for (h = 0; h < HASH_SIZE; h++) {
        this->block->freeHash[h].head.u.next_free = &(this->block->freeHash[h].head);
        this->block->freeHash[h].tail.prev_free = &(this->block->freeHash[h].head);
        this->block->freeHash[h].head.size = 0;
        //this->block->freeHash[h].tail.size = 0;
        this->block->freeHash[h].numFrags = 0;
    }
    insert_free(this->block->firstFrag);
    return 0;
}

