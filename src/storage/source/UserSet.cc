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
 * UserSet.cc
 *
 *  Created on: Dec 21, 2015
 *      Author: Jia
 */

#ifndef USER_SET_CC
#define USER_SET_CC


#include "UserSet.h"
#include "PartitionPageIterator.h"
#include "SetCachePageIterator.h"
#include <string.h>

/**
 * Create a UserSet instance, need to set file, and open file later
 */
UserSet::UserSet( pdb :: PDBLoggerPtr logger, SharedMemPtr shm, NodeID nodeId,
		DatabaseID dbId, UserTypeID typeId, SetID setId, string setName,
		PageCachePtr pageCache, LocalityType localityType, LocalitySetReplacementPolicy policy, OperationType operation, DurabilityType durability, PersistenceType persistence):LocalitySet(localityType, policy, operation, durability, persistence) {
	//cout<<"creating set...\n";
	this->logger = logger;
	this->shm = shm;
	this->nodeId = nodeId;
	this->dbId = dbId;
	this->typeId = typeId;
	this->setId = setId;
	this->setName = setName;
	this->pageCache = pageCache;
        this->inputBufferPage = nullptr;
        this->lastFlushedPageId = (unsigned int) (-1);
        this->dirtyPagesInPageCache = new unordered_map<PageID, FileSearchKey>();
        pthread_mutex_init(&this->dirtyPageSetMutex, nullptr);
        this->isPinned = false;
}


/**
 * Create a UserSet instance.
 */
UserSet::UserSet(size_t pageSize,
		 pdb :: PDBLoggerPtr logger, SharedMemPtr shm,
		NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
		string setName, PartitionedFilePtr file,
		PageCachePtr pageCache, LocalityType localityType, LocalitySetReplacementPolicy policy, OperationType operation, DurabilityType durability, PersistenceType persistence):LocalitySet(localityType, policy, operation, durability, persistence) {
	//cout<<"creating set...\n";
	this->logger = logger;
	this->shm = shm;
	this->nodeId = nodeId;
	this->dbId = dbId;
	this->typeId = typeId;
	this->setId = setId;
	this->setName = setName;
	this->file = file;
        if (this->file->getNumFlushedPages() == 0) {
            this->lastFlushedPageId = (unsigned int)(-1); //0xFFFFFFFF
        } else {
            this->lastFlushedPageId = file->getLastFlushedPageID();
            this->seqId.initialize(this->lastFlushedPageId + 1);
        }
        //cout << "LastFlushedPageId =" << this->lastFlushedPageId<<"\n";
	this->pageCache = pageCache;
	//open files for read and write
	//cout<<"opening files in the partitioned file...\n";
	this->file->openAll();
        this->inputBufferPage = nullptr;
        this->dirtyPagesInPageCache = new unordered_map<PageID, FileSearchKey>();
        pthread_mutex_init(&this->dirtyPageSetMutex, nullptr);
        this->isPinned = false;
}


/**
 * Destructor.
 */
UserSet::~UserSet() {
    delete this->dirtyPagesInPageCache;
}



/**
 * Get page from set.
 * Step 1. check whether the page is already in cache using cache key, if so return it.
 * Step 2. check whether the page is flushed to disk file, if so, load it to cache, and return it.
 */
PDBPagePtr UserSet::getPage(FilePartitionID partitionId,
		unsigned int pageSeqInPartition, PageID pageId) {

	/**
	 * check whether the page is already in cache using cache key, if so return it.
	 * otherwise, check whether the page is flushed to disk file, if so, load it to cache, and return it.
	 */
	return this->pageCache->getPage(this->file, partitionId, pageSeqInPartition,
			pageId, false, this);
}

PDBPagePtr UserSet::addPage() {
    //cout << "To add page...\n";
    //this->logger->writeLn("UserSet: to add page...");
    PageID pageId = seqId.getNextSequenceID();
    //cout << "PDBPagePtr: pageId=" << pageId << "\n";
    //this->logger->writeLn("UserSet: pageId generated =");
    //this->logger->writeInt(pageId);
    CacheKey key;
    key.dbId = this->dbId;
    key.typeId = this->typeId;
    key.setId = this->setId;
    key.pageId = pageId;
    PDBPagePtr page = this->pageCache->getNewPage(this->nodeId, key, this);
    //cout << "PDBPagePtr: page->getPageID()=" << page->getPageID() << "\n";
    //this->logger->writeLn("UserSet: pageId set =");
    //this->logger->writeInt(pageId);
    if(page == nullptr ) {
        return nullptr;
    }
    //cout << "Got page from buffer pool!\n";
    page->preparePage();
    //cout << "Page header prepared!\n";
    this->addPageToDirtyPageSet(page->getPageID());
    return page;
}

PDBPagePtr UserSet::addPageByRawBytes(size_t sharedMemOffset) {
    return nullptr;
}


/**
 * Get a set of iterators for scanning the data in the set.
 * The set of iterators will include:
 * -- 1 iterator to scan data in page cache;
 * -- K iterators to scan data in file partitions, assuming there are K partitions.
 */
vector<PageIteratorPtr> * UserSet::getIterators() {

        this->cleanDirtyPageSet();   
        this->lockDirtyPageSet(); 
	vector<PageIteratorPtr> * retVec = new vector<PageIteratorPtr>();
	PageIteratorPtr iterator = nullptr;
	iterator = make_shared<SetCachePageIterator>(this->pageCache, this);
	if (iterator != nullptr) {
		retVec->push_back(iterator);
	}
	if (this->file->getFileType() == FileType::SequenceFileType) {
		SequenceFilePtr seqFile = dynamic_pointer_cast<SequenceFile>(
				this->file);
		iterator = make_shared<PartitionPageIterator>(this->pageCache, file,
				(FilePartitionID) 0, this);
		retVec->push_back(iterator);
	} else {
		PartitionedFilePtr partitionedFile = dynamic_pointer_cast<
				PartitionedFile>(this->file);
		int numPartitions = partitionedFile->getNumPartitions();
		int i = 0;
		for (i = 0; i < numPartitions; i++) {
			if(partitionedFile->getMetaData()->getPartition(i)->getNumPages() > 0) {
				iterator = make_shared<PartitionPageIterator>(this->pageCache, file,
					(FilePartitionID) i, this);
				retVec->push_back(iterator);
			}
		}
	}
        this->unlockDirtyPageSet();
	return retVec;
}

//user MUST guarantee that the size of buffer is large enough for dumping all data in the set. 
void UserSet::dump (char * buffer) {
   setPinned(true);
   vector<PageIteratorPtr> * iterators = this->getIterators();
   int numIterators = iterators->size();
   int i;
   char * cur = buffer;
   for (i = 0; i < numIterators; i++) {
       PageIteratorPtr curIter = iterators->at(i);
       while(curIter->hasNext()) {
           PDBPagePtr curPage = curIter->next();
           if(curPage != nullptr) {
               //cout << "dump page for pageId=" << curPage->getPageID() << "\n";
               memcpy(cur, curPage->getRawBytes(), curPage->getRawSize());
               cur = cur + curPage->getRawSize(); 
               curPage->decRefCount();
           }
       }
   }
   setPinned(false);
}


//thread-safe
void UserSet::cleanDirtyPageSet() {
    this->lockDirtyPageSet();
    auto iter = this->getDirtyPageSet()->begin();
    while(iter != this->getDirtyPageSet()->end()) {
        if(iter->second.inCache == false) {
            iter = this->getDirtyPageSet()->erase(iter);
        }else {
            iter ++;
        }
    }
    this->unlockDirtyPageSet();
}


void UserSet::flushDirtyPages() {
    //this->lockDirtyPageSet();
    auto iter = this->getDirtyPageSet()->begin();
    while (iter != this->getDirtyPageSet()->end()) {
        if(iter->second.inCache == true) {
            CacheKey key;
            key.dbId = this->getDbID();
            key.typeId = this->getTypeID();
            key.setId = this->getSetID();
            key.pageId = iter->first;
            this->pageCache->flushPageWithoutEviction(key);
            //cout << "page flushed and to erase it \n";
            iter = this->getDirtyPageSet()->erase(iter);                        
        } else {
            iter ++;
        }
    }
    //this->unlockDirtyPageSet();
}

#endif
