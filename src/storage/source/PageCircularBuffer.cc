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
 * File:   PageCircularBuffer.cc
 * Author: Jia
 *
 */


#ifndef PAGE_CIRCULAR_BUFFER_CC
#define PAGE_CIRCULAR_BUFFER_CC

#include "PDBDebug.h"
#include "PageCircularBuffer.h"
#include <string>
#include <iostream>
#include <pthread.h>
#include <sched.h>

PageCircularBuffer::PageCircularBuffer(unsigned int bufferSize, pdb :: PDBLoggerPtr logger) {
    this->maxArraySize = bufferSize + 1;
    this->logger = logger;
    this->closed = false;
    this->initArray();
    pthread_mutex_init(&(this->mutex), NULL);
    pthread_mutex_init(&(this->addPageMutex), NULL);
    pthread_cond_init(&(this->cond), NULL);
}

PageCircularBuffer::~PageCircularBuffer() {
    //the buffer is not responsible for freeing the elements in the buffer
    delete[] this->pageArray;
    pthread_mutex_destroy(&(this->mutex));
    pthread_cond_destroy(&(this->cond));
}

int PageCircularBuffer::initArray() {
    this->pageArray = new PDBPagePtr[this->maxArraySize];
    if (this->pageArray == nullptr) {
        cout << "PageCircularBuffer: Out of Memory in Heap.\n";
        this->logger->writeLn("PageCircularBuffer: Out of Memory in Heap.");
        return -1;
    }
    unsigned int i;
    for (i = 0; i<this->maxArraySize; i++) {

        this->pageArray[i] = nullptr;

    }
    this->pageArrayHead = 0;
    this->pageArrayTail = 0;
    return 0;
}

//in our case, more than one producer will add pages to the tail of the blocking queue
int PageCircularBuffer::addPageToTail(PDBPagePtr page) {
    pthread_mutex_lock(&(this->addPageMutex));
    int i = 0;
    while (this->isFull()) {
        i ++;
        if (i%10000000==0) {
            this->logger->info(std :: to_string(i) + std :: string(":PageCircularBuffer: array is full."));
        }
        pthread_cond_signal(&(this->cond));
        sched_yield(); //TODO: consider to use another conditional variable
    }

    this->logger->writeLn("PageCircularBuffer:got a place.");
    this->pageArrayTail = (this->pageArrayTail + 1) % this->maxArraySize;
    this->pageArray[this->pageArrayTail] = page;
    pthread_mutex_unlock(&(this->addPageMutex));
    pthread_mutex_lock(&(this->mutex));
    if(this->getSize() <= 2) {//TODO <= numThreads? or not necessary
        pthread_cond_broadcast(&(this->cond));
    } else {
    	pthread_cond_signal(&(this->cond));
    }
    pthread_mutex_unlock(&(this->mutex));
    return 0;

}

//there will be multiple consumers, so we need to guard the blocking queue

PDBPagePtr PageCircularBuffer::popPageFromHead() {
    pthread_mutex_lock(&(this->mutex));
    if (this->isEmpty() && (this->closed == false)) {
        this->logger->writeLn("PageCircularBuffer: array is empty.");
        pthread_cond_wait(&(this->cond), &(this->mutex));
    }
    if(!this->isEmpty()) {
        this->logger->debug("PageCircularBuffer: not empty, return the head element");
        this->pageArrayHead = (this->pageArrayHead + 1) % this->maxArraySize;
        PDBPagePtr ret = this->pageArray[this->pageArrayHead];
        this->pageArray[this->pageArrayHead] = nullptr;
        pthread_mutex_unlock(&(this->mutex));
        return ret;
    } else {    
        pthread_mutex_unlock(&(this->mutex));
        return nullptr;
    } 
}
//not thread-safe

bool PageCircularBuffer::isFull() {
    return (this->pageArrayHead == (this->pageArrayTail + 1) % this->maxArraySize);
}
//not thread-safe

bool PageCircularBuffer::isEmpty() {
    PDB_COUT << "this->pageArrayHead=" << this->pageArrayHead << std :: endl;
    this->logger->debug (std :: string("this->pageArrayHead=")+std :: to_string(this->pageArrayHead));
    this->logger->debug (std :: string("this->pageArrayTail=")+std :: to_string(this->pageArrayTail));
    PDB_COUT << "this->pageArrayTail=" << this->pageArrayTail << std :: endl;
    return (this->pageArrayHead == this->pageArrayTail);
}
//not thread-safe

unsigned int PageCircularBuffer::getSize() {
    return (this->pageArrayTail - this->pageArrayHead + this->maxArraySize) % this->maxArraySize;
}

void PageCircularBuffer::close() {
    pthread_mutex_lock(&(this->mutex));
    this->closed = true;
    pthread_cond_broadcast(&(this->cond));
    pthread_mutex_unlock(&(this->mutex));
}


void PageCircularBuffer::open() {
    this->closed = false;
}
#endif
