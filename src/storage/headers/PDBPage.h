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
 * File:   PDBPage.h
 * Author: Jia
 *
 * Created on September 27, 2015, 2:22 PM
 */

#ifndef PDBPAGE_H
#define	PDBPAGE_H

#include "PDBObject.h"
#include "DataTypes.h"
#include "PDBLogger.h"
#include <memory>
#include <pthread.h>
#include <stdlib.h>
#include <iostream>
using namespace std;
// create a smart pointer for PDBBufferPagePtr objects
class PDBPage;
typedef shared_ptr <PDBPage> PDBPagePtr;

/**
 * This class implements PDBPage that is a fixed size (e.g. 64MB) piece of data allocated in shared memory.
 * A PDBPage usually consists of two parts:
 * 1) A page header that often consists of following fields:
 * - NodeID
 * - DatabaseID
 * - UserTypeID
 * - PageID
 * - NumObjects
 * - NumMiniPages
 *
 * 2) A page body that consists of a series of <object raw data size, object raw data> pairs ,
 * with each pair aligned with miniPage size (often set to be cacheline size).
 *
 * A page at frontend will be setup by the PageCache instance.
 * A page at backend will be setup by the DataProxy instance and used via PageHandle instance.
 *
 */
class PDBPage {

public:
	/**
	 * Create a PDBPage instance.
	 */
    PDBPage(char * dataIn, NodeID dataNodeID, DatabaseID dataDbID,
            UserTypeID dataTypeID, SetID setID, PageID pageID, size_t dataSize, 
            size_t offset, int internalOffset=0);
    ~PDBPage();
    /**
     * Free page data from shared memory.
     */
    void freePage();

    /**
     * Prepare page head.
     */
    void preparePage();


    inline void incRefCount() {
        pthread_mutex_lock(&this->refCountMutex);
        this->refCount++;
        this->pinned = true;
        pthread_mutex_unlock(&this->refCountMutex);
    }

    inline void decRefCount() {
        pthread_mutex_lock(&this->refCountMutex);
        this->refCount--;
        if (this->refCount < 0) {
            //there is a problem:
            //reference count should always >= 0
            pthread_mutex_unlock(&this->refCountMutex);
            std :: cout << "Fatal Error: page reference count < 0" << std :: endl;
            exit(-1);
        } else if (this->refCount == 0) {
            this->setPinned(false);
        }
        pthread_mutex_unlock(&this->refCountMutex);
    }

    /**
     * Allocate an empty memory area with variable size from the current offset as a special object, that can be used to implement variable-size small pages.
     */
    inline void * addVariableBytes(size_t size) {
        //this->writeLock();
        size_t remainSize = this->size - this->curAppendOffset;
        if (remainSize < size + sizeof(size_t)) {
            //no room in the current page
            //this->writeUnlock();
            return nullptr;
        }

        //write data
        //get pointer to append position
        char * cur = this->rawBytes + this->curAppendOffset;
        //write the size
        *((size_t *) cur) = size;
        void * retPos = cur + sizeof(size_t);
        cur = (char *)retPos + size;
        this->curAppendOffset = cur - this->rawBytes;
        this->numObjects++;
        //this->writeUnlock();
        return retPos;
    }




    /*****To Comply with Chris' interfaces******/

    void * getBytes();

    size_t getSize();

    void unpin(); 
    
    /********************Mutexes*****************/

    //To lock page for read.
    void readLock();

    //To release the read lock.
    void readUnlock();

    //To lock page for write.
    void writeLock();

    //To release the write lock.
    void writeUnlock();


    /************Simple getters/setters**********/

    //To return NodeID
    NodeID getNodeID() const {
        return nodeID;
    }

    //To return DatabaseID.
    DatabaseID getDbID() const {
        return dbID;
    }

    //To return UserTypeID.
    UserTypeID getTypeID() const {
        return typeID;
    }

    //To return SetID.
    SetID getSetID() const {
        return setID;
    }

    //To return PageID
    PageID getPageID() const {
        return pageID;
    }

    //To return raw data.
    char* getRawBytes() const {
        return rawBytes;
    }

    //To return size of raw data.
    size_t getRawSize() const {
        return size;
    }

    //To return offset in shared memory.
    size_t getOffset() const {
        return offset;
    }

    //To return number of miniPages used to write page header.
    int getNumHeadMiniPages() const {
        return numHeadMiniPages;
    }

    //To return number of miniPages that has been written to the page.
    int getNumMiniPages() const {
        return numMiniPages;
    }


    //To return the miniPage size.
    size_t getMiniPageSize() const {
        return miniPageSize;
    }

    //To return page header size
    //(net size, not the size that is aligned with miniPage).
    size_t getHeadSize() const {
        return headSize;
    }

    //To return last accessed sequence Id of this page.
    long getAccessSequenceId() const {
        return accessSequenceId;
    }

    //To return the reference count of this page.
    int getRefCount() {
        return this->refCount;
    }

    //Return whether page is pinned.
    //Page is pinned if reference count > 0.
    //Once page is unpinned, we can flush the page to disk, or evict the page from cache.
    bool isPinned() {
        return this->pinned;
    }

    //Return whether page is dirty (i.e. hasn't been flushed to disk yet).
    bool isDirty() {
        return this->dirty;
    }


    //Return whether page is in flush
    bool isInFlush() {
       return this->inFlush;
    }

    //Return whether page is in eviction
    bool isInEviction() {
       return this->inEviction;
    }
   
    unsigned int getPageSeqInPartition() const {
       return pageSeqInPartition;
    }

    FilePartitionID getPartitionId() const {
       return partitionId;
    }

    //To set NodeID.
    void setNodeID(NodeID nodeID) {
        this->nodeID = nodeID;
    }

    //To set DatabaseID.
    void setDbID(DatabaseID dbID) {
        this->dbID = dbID;
    }

    //To set TypeID.
    void setTypeID(UserTypeID typeID) {
        this->typeID = typeID;
    }

    //To set SetID.
    void setSetID(SetID setID) {
        this->setID = setID;
    }

    //To set PageID.
    void setPageID(PageID pageID) {
        this->pageID = pageID;
    }

    //To set raw data.
    void setRawBytes(char* rawBytes) {
        this->rawBytes = rawBytes;
    }

    //To set raw data size.
    void setSize(size_t size) {
        this->size = size;
    }

    //To set offset of raw data in shared memory.
    void setOffset(size_t offset) {
        this->offset = offset;
    }


    //To set number of miniPages to store page header.
    void setNumHeadMiniPages(int numHeadMiniPages) {
        this->numHeadMiniPages = numHeadMiniPages;
    }

    //To set number of miniPages that has been written to the page.
    void setNumMiniPages(int numMiniPages) {
        this->numMiniPages = numMiniPages;
    }


    //To set miniPage size.
    void setMiniPageSize(size_t miniPageSize) {
        this->miniPageSize = miniPageSize;
    }

    //To set the page header size (net size, not the size that is aligned with miniPage)
    void setHeadSize(size_t headSize) {
        this->headSize = headSize;
    }

    //To set last accessed sequence Id for the page.
    void setAccessSequenceId(long accessSequenceId) {
        this->accessSequenceId = accessSequenceId;
    }

    //To set whether the page is pinned or not.
    void setPinned(bool isPinned) {
        this->pinned = isPinned;
    }

    //To set whether the page is dirty or not.
    void setDirty(bool dirty) {
	this->dirty = dirty;	
    }

    //To set whether the page is in flush or not.
    void setInFlush(bool inFlush) {
        this->inFlush = inFlush;
    }

    //To set whether the page is in eviction or not
    void setInEviction(bool inEviction) {
        this->inEviction = inEviction;
    }

    void setPageSeqInPartition(unsigned int pageSeqInPartition) {
	this->pageSeqInPartition = pageSeqInPartition;
     }

    void setPartitionId(FilePartitionID partitionId) {
	this->partitionId = partitionId;
    }

    void setInternalOffset(int offset) {
        this->internalOffset = offset;
    }

    int getInternalOffset() {
        return this->internalOffset;
    }

private:
    char * rawBytes;
    size_t offset; //used in shared memory
    int numHeadMiniPages;
    int numMiniPages;
    unsigned int numObjects;
    size_t miniPageSize;
    size_t headSize;
    NodeID nodeID;
    DatabaseID dbID;
    UserTypeID typeID;
    SetID setID;
    PageID pageID;
    size_t size;
    int refCount;
    bool pinned;
    bool dirty;
    pthread_mutex_t refCountMutex;
    pthread_rwlock_t flushLock;
    long accessSequenceId;
    bool inFlush;
    bool inEviction;

    //Below info can only be filled when the page has been loaded to cache after flushed to a disk file.
    //Otherwise, they will be unsigned int(-1).
    //When backend wants to pin a page, it will use pageId to check whether the page is already in cache.
    //If the page is not in cache, then it will be loaded to cache using partitionId and pageSeqInPartition.
    FilePartitionID partitionId;
    unsigned int pageSeqInPartition;

    //Offset from last append position to start of raw bytes
    size_t curAppendOffset;
    int internalOffset;
};




#endif	/* PDBPAGE_H */

