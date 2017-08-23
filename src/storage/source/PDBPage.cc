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
 * File:   PDBPage.cc
 * Author: Jia
 *
 */

#ifndef PDB_Page_C
#define PDB_Page_C
#include "PDBPage.h"
#include <cstring>
#include <stdlib.h>
#include <iostream>


PDBPage::PDBPage(char * dataIn, NodeID dataNodeID, DatabaseID dataDbID,
        UserTypeID dataTypeID, SetID dataSetID, PageID dataPageID, size_t dataSize,
        size_t shmOffset, int internalOffset) {
    //cout << "Page Data Offset = " << shmOffset << "\n";
    rawBytes = dataIn;
    nodeID = dataNodeID;
    dbID = dataDbID;
    typeID = dataTypeID;
    setID = dataSetID;
    pageID = dataPageID;
    size = dataSize;
    offset = shmOffset;
    numObjects = 0;
    this->curAppendOffset = 0;
    this->refCount = 0;
    this->pinned = true;
    this->dirty = false;
    this->inFlush = false;
    this->partitionId = (FilePartitionID)(-1);
    this->pageSeqInPartition = (unsigned int)(-1);
    pthread_mutex_init(&(this->refCountMutex), nullptr);
    pthread_rwlock_init(&(this->flushLock), nullptr);
    this->internalOffset = internalOffset;
    char * refCountBytes = this->rawBytes + (sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID));
    *((int *) refCountBytes) = 0;
}

PDBPage::~PDBPage() {
    //cout<<"PDBPage: freeing page...";
    freePage();
    //cout<<"PDBPage: cleaning locks...";
    pthread_mutex_destroy(&(this->refCountMutex));
    pthread_rwlock_destroy(&(this->flushLock));
    //cout<<"PDBPage: locks cleaned.";
}

/**
 * Prepare page head.
 */

void PDBPage::preparePage() {
   char * cur = this->rawBytes;
   * ((NodeID *) cur) = nodeID;
   cur = cur + sizeof (NodeID);
   * ((DatabaseID *) cur) = dbID;
   cur = cur + sizeof (DatabaseID);
   * ((UserTypeID *) cur) = typeID;
   cur = cur + sizeof (UserTypeID);
   * ((SetID *) cur) = setID;
   cur = cur + sizeof (SetID);
   * ((PageID *) cur) = pageID;
   cur = cur + sizeof (PageID);
   *((int *) cur) = 0;
   this->curAppendOffset = sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID) + sizeof(int);
   return;     
}


void PDBPage::readLock() {
    pthread_rwlock_rdlock(&(this->flushLock));
}

void PDBPage::readUnlock() {
    pthread_rwlock_unlock(&(this->flushLock));
}

void PDBPage::writeLock() {
    pthread_rwlock_wrlock(&(this->flushLock));
}

void PDBPage::writeUnlock() {
    pthread_rwlock_unlock(&(this->flushLock));
}

void * PDBPage::getBytes() {

        return this->rawBytes + sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID) + sizeof(int);

}

size_t PDBPage::getSize() {

        return this->size - (sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID) + sizeof(int));

}

void  PDBPage::unpin() {
    this->decRefCount();
}

void PDBPage::freePage() {
    //this->writeLock();
    //cout<<"PDBPage: got lock for freeing page data...";
    if (rawBytes != nullptr) {
        //we always free page data by SharedMem class when page is flushed or evicted, and we do not free page data here
        //If it comes to here, there must be a problem. Shared memory should already be freed, there could be memory leaks
        rawBytes = nullptr;
    }
    //cout<<"PDBPage: page data freed...";
    //this->writeUnlock();
    nodeID = -1;
    dbID = -1;
    typeID = -1;
    setID = -1;
    pageID = -1;
    offset = 0;
    size = 0;
    numObjects = 0;
    internalOffset = 0;
    //cout<<"PDBPage: page data reset...";
}

#endif






