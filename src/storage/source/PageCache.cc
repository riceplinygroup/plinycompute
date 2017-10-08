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
 * File:   PageCache.cc
 * Author: Jia
 *
 */

#ifndef PAGE_CACHE_CC
#define PAGE_CACHE_CC

#include "PDBDebug.h"
#include "PageCache.h"
#include "PDBEvictWork.h"

#include <queue>
#include <stdlib.h>
#include <sched.h>
using namespace std;

#define WARN_THRESHOLD 0.9
#ifndef EVICT_STOP_THRESHOLD
    #define EVICT_STOP_THRESHOLD 0.9
#endif

PageCache::PageCache(ConfigurationPtr conf, pdb :: PDBWorkerQueuePtr workers, PageCircularBufferPtr flushBuffer,
		pdb :: PDBLoggerPtr logger, SharedMemPtr shm, CacheStrategy strategy) {
	this->cache = new unordered_map<CacheKey, PDBPagePtr, CacheKeyHash,
			CacheKeyEqual>();
	this->conf = conf;
	this->workers = workers;
	pthread_mutex_init(&this->countLock, nullptr);
        pthread_mutex_init(&this->cacheMutex, nullptr);
        pthread_mutex_init(&this->evictionMutex, nullptr);
	pthread_rwlock_init(&this->evictionAndFlushLock, nullptr);
	this->accessCount = 0;
	this->inEviction = false;
	this->maxSize = conf->getShmSize();
	this->size = 0;
	this->warnSize = (this->maxSize) * WARN_THRESHOLD;

        std :: cout << "maxSize=" << maxSize << std :: endl;

	this->evictStopSize = (this->maxSize) * EVICT_STOP_THRESHOLD;

        std :: cout << "evictStopSize=" << evictStopSize << std :: endl;
        
        if (this->evictStopSize >= (shm->getShmSize()-6*conf->getMaxPageSize())) {
           this->evictStopSize = shm->getShmSize()-6*conf->getMaxPageSize();
           std :: cout << "evictStopSize=" << evictStopSize << std :: endl;
           if(this->evictStopSize <= conf->getMaxPageSize()) {
               this->evictStopSize = conf->getMaxPageSize();
               std :: cout << "evictStopSize=" << evictStopSize << std :: endl;
           }
        }
        std :: cout << "PageCache: EVICT_STOP_SIZE is automatically tuned to be " << this->evictStopSize << std :: endl;
        this->flushBuffer = flushBuffer;
	this->logger = logger;
	this->shm = shm;
        this->strategy = strategy;
        this->priorityList = new vector<list<LocalitySetPtr> *>();
        int i;
        for (i = 0; i < 6; i ++) {
            list<LocalitySetPtr> * curList = new list<LocalitySetPtr>();
            this->priorityList->push_back(curList);
        }
	logger->writeLn("LRUPageCache: warn size:");
	logger->writeInt(this->warnSize);
	logger->writeLn("LRUPageCache: stop size:");
	logger->writeInt(this->evictStopSize);
}

PageCache::~PageCache() {
	delete this->cache;
	pthread_mutex_destroy(&this->countLock);
        pthread_mutex_destroy(&this->cacheMutex);
        pthread_mutex_destroy(&this->evictionMutex);
	pthread_rwlock_destroy(&this->evictionAndFlushLock);
}

//Cache the page with specified name and buffer;
void PageCache::cachePage(PDBPagePtr page, LocalitySet* set) {
	if (page == nullptr) {
		logger->writeLn("LRUPageCache: null page.");
	}
	CacheKey key;
	key.dbId = page->getDbID();
	key.typeId = page->getTypeID();
	key.setId = page->getSetID();
	key.pageId = page->getPageID();
        pthread_mutex_lock(&this->cacheMutex);
	if (this->cache->find(key) == this->cache->end()) {
		pair<CacheKey, PDBPagePtr> pair = make_pair(key, page);
		this->cache->insert(pair);
		this->size += page->getRawSize()+512;
	} else {
		logger->writeLn("LRUPageCache: page was there already.");
	}
        pthread_mutex_unlock(&this->cacheMutex);
        if(set != nullptr) {
            set->addCachedPage(page);
        }
}

//If there is sufficient room in shared memory, allocate the buffer as required
//Otherwise, try to evict a page from shared memory.
//It will block until data can be allocated.
char * PageCache::allocateBufferFromSharedMemoryBlocking(size_t size, int & alignOffset) {
        //below function is thread-safe
	char* data = (char *) this->shm->mallocAlign(size, 512, alignOffset);
	//Dangerous: dead loop
	while (data == nullptr) {
		this->logger->info(
				"LRUPageCache: out of memory in off-heap pool, start eviction.");
#ifdef PROFILING_CACHE
                std :: cout << "Out of memory in shared memory pool, trying to allocate " << size << " data" << std :: endl;
#endif
		if (this->inEviction == false) {
			this->evict();
		} else {
			this->logger->info(
					"waiting for eviction work to evict at least one page.");
			sched_yield();
		}
		data = (char *) this->shm->mallocAlign(size, 512, alignOffset);
	}
          
        PDB_COUT << "page allocated!\n";
	return data;
}

//Try to allocate buffer of required size from shared memory, if no room, return nullptr.
char * PageCache::tryAllocateBufferFromSharedMemory(size_t size, int & alignOffset) {
	char* data = (char *) this->shm->mallocAlign(size, 512, alignOffset);
        if(data == nullptr) {
            this->logger->writeLn("LRUPageCache: out of memory in off-heap pool, start eviction.");
            this->evict(); 
	    data = (char *) this->shm->mallocAlign(size, 512, alignOffset);
        }
        return data;
}

//Lock for eviction.
void PageCache::evictionLock() {
        pthread_rwlock_wrlock(&this->evictionAndFlushLock);
}

//Unlock for eviction.
void PageCache::evictionUnlock() {
        pthread_rwlock_unlock(&this->evictionAndFlushLock);
}

//Lock for flushing.
void PageCache::flushLock() {
        pthread_rwlock_rdlock(&this->evictionAndFlushLock);
}

//Unlock for flushing.
void PageCache::flushUnlock() {
        pthread_rwlock_unlock(&this->evictionAndFlushLock);
}

PDBPagePtr PageCache::buildAndCachePageFromFileHandle(int handle, size_t size, NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId, PageID pageId) {
     int offset;
     char * buffer = allocateBufferFromSharedMemoryBlocking(size, offset);
     ssize_t readSize = read(handle, buffer, size);
     if (readSize <= 0) {
         std :: cout << "PageCache: Read failed" << std :: endl;
         return nullptr;
     }
     PDBPagePtr page = make_shared<PDBPage>(buffer, nodeId, dbId, typeId, setId, pageId, size, shm->computeOffset(buffer), offset);
     return page;
}

//Build a PDBPage instance based on the info of file and info parsed from loaded page data.
PDBPagePtr PageCache::buildPageFromSharedMemoryData(PDBFilePtr file,
		char* pageData, FilePartitionID partitionId, unsigned int pageSeqInPartition, int internalOffset, size_t pageSize) {
	if(pageData == nullptr) {
		return nullptr;
	}
	char * cur = pageData + sizeof(NodeID) + sizeof(DatabaseID)
			+ sizeof(UserTypeID) + sizeof(SetID);
	PageID pageId = (PageID)(*(PageID *) cur);
	cur = cur + sizeof(PageID);
	int numObjects = (int) (*(int *) cur);
	//create a new PDBPagePtr
	PDBPagePtr page = make_shared<PDBPage>(pageData, file->getNodeId(),
			file->getDbId(), file->getTypeId(), file->getSetId(), pageId,
			pageSize, shm->computeOffset(pageData), internalOffset, numObjects);
	if (page == nullptr) {
		this->logger->error("Fatal Error: PageCache: out of memory in heap.");
                std :: cout << "FATAL ERROR: PageCache out of memory" << std :: endl;
		exit(-1);
	}
	page->setNumObjects(numObjects);
	page->setPartitionId(partitionId);
	page->setPageSeqInPartition(pageSeqInPartition);
	return page;
}

//Load the page in a PDBFile (SequenceFile or PartitionedFile) into memory
//If the file is SequenceFile, partition id will be ignored.
PDBPagePtr PageCache::loadPage(PDBFilePtr file, FilePartitionID partitionId,
		unsigned int pageSeqInPartition, bool sequential) {
	//check file type
	if (file->getFileType() == FileType::SequenceFileType) {
		return this->loadPage(dynamic_pointer_cast<SequenceFile>(file),
				(PageID) pageSeqInPartition);
	} else {
		//It's a PartitionedFile instance.
		PartitionedFilePtr curFile = dynamic_pointer_cast<PartitionedFile>(
				file);
		//Check the partition size.
		unsigned int numPagesInPartition = (unsigned int)curFile->getMetaData()->getPartition(
				partitionId)->getNumPages();

		if (pageSeqInPartition >= numPagesInPartition) {
			return nullptr;
		}
                int internalOffset = 0;
                size_t pageSize = file->getPageSize();
		//allocate memory for the page from shared memory pool, if there is no free page,  it will blocking and evicting, until there is new room.
		char * pageData = this->allocateBufferFromSharedMemoryBlocking(
				pageSize, internalOffset);
		//seek to page
		if(sequential == true) {
			curFile->loadPageFromCurPos(partitionId, pageSeqInPartition, pageData,
				pageSize);
		} else {
			curFile->loadPage(partitionId, pageSeqInPartition, pageData,
				pageSize);
		}
		//build page from loaded data
		return this->buildPageFromSharedMemoryData(file, pageData, partitionId, pageSeqInPartition, internalOffset, pageSize);

	}
	return nullptr;
}

//Load the page in a SequenceFile into memory;
PDBPagePtr PageCache::loadPage(SequenceFilePtr file, PageID pageId) {
	if (pageId > file->getLastFlushedPageID()) {
		this->logger->writeLn(
				"LRUPageCache: page is still in input buffer, and hasn't been flushed yet.");
		return nullptr;
	}
        int internalOffset = 0;
	//allocate a page from shared memory pool, if there is no free page, it will blocking and evicting, until there is new room.
        size_t pageSize = file->getPageSize();
	char* pageData = this->allocateBufferFromSharedMemoryBlocking(
			pageSize, internalOffset);
	//seek to the pageId
	file->loadPage(pageId, pageData, pageSize);
	//build page from loaded data
	return this->buildPageFromSharedMemoryData(file, pageData, 0, pageId, internalOffset, pageSize);

}


//Remove page specified by Key from cache hashMap.
//This function will be used by the flushConsumer thread.
bool PageCache::removePage(CacheKey key) {
    pthread_mutex_lock(&this->cacheMutex);
    if (this->containsPage(key) == false) {
        pthread_mutex_unlock(&this->cacheMutex);
        return false;
    }
    size_t pageSizeAllocated = this->cache->at(key)->getRawSize() + 512;
    this->cache->erase(key);
    this->size -= pageSizeAllocated;
    pthread_mutex_unlock(&this->cacheMutex);
    return true;
}

//Free page data and Remove page specified by Key from cache hashMap.
//This function will be used by the UserSet::clear() method.
bool PageCache::freePage(PDBPagePtr curPage) {
    CacheKey key;
    key.dbId = curPage->getDbID();
    key.typeId = curPage->getTypeID();
    key.setId = curPage->getSetID();
    key.pageId = curPage->getPageID();

    pthread_mutex_lock(&this->cacheMutex);
    if (this->containsPage(key) == false) {
        pthread_mutex_unlock(&this->cacheMutex);
        return false;
    }
    size_t pageSizeAllocated = this->cache->at(key)->getRawSize() + 512;
    cache->erase(key);
    this->size -= pageSizeAllocated;
    pthread_mutex_unlock(&this->cacheMutex);
    this->shm->free(curPage->getRawBytes()-curPage->getInternalOffset(), curPage->getRawSize()+512);
    curPage->setOffset(0);
    curPage->setRawBytes(nullptr);
    return true;
}



//Below method can be used for all PDBFile instances, include sequence file and partitioned file.
//note that below method will cause cached page reference count ++;
PDBPagePtr PageCache::getPage(PartitionedFilePtr file, FilePartitionID partitionId, unsigned int pageSeqInPartition,
		PageID pageId, bool sequential, LocalitySet* set) {
	CacheKey key;
	key.dbId = file->getDbId();
	key.typeId = file->getTypeId();
	key.setId = file->getSetId();
	key.pageId = pageId;
	PDBPagePtr page;
        
        if((partitionId == (unsigned int)(-1)) || (pageSeqInPartition == (unsigned int)(-1)))      {
                PageIndex pageIndex = file->getMetaData()->getPageIndex(pageId);
                partitionId = pageIndex.partitionId;
                pageSeqInPartition = pageIndex.pageSeqInPartition;
        }
        //Assumption: At one time, for a page, only one thread will try to load it.
        //Above assumption is guaranteed by the front-end scan model.
        pthread_mutex_lock(&this->evictionMutex);
        this->evictionLock();
        pthread_mutex_lock(&this->cacheMutex);
	if (this->containsPage(key) != true) {
                pthread_mutex_unlock(&this->cacheMutex);
                this->evictionUnlock();
                pthread_mutex_unlock(&this->evictionMutex);
		page = this->loadPage(file, partitionId, pageSeqInPartition, sequential);
		if (page == nullptr) {
			return nullptr;
		}
                pthread_mutex_lock(&this->countLock);
                page->setAccessSequenceId(this->accessCount);
                this->accessCount++;
                pthread_mutex_unlock(&this->countLock);
                
                pthread_mutex_lock(&this->evictionMutex);
		this->cachePage(page, set);
                page->setPinned(true);
                page->setDirty(false);
                page->incRefCount();
                pthread_mutex_unlock(&this->evictionMutex);
	} else {
		page = this->cache->at(key); 
                pthread_mutex_unlock(&this->cacheMutex);
                if (page == nullptr) {
                   std :: cout << "WARNING: PartitionPageIterator get nullptr in cache.\n" << std :: endl;
                   logger->warn("PartitionPageIterator get nullptr in cache.");
                   this->evictionUnlock();
                   pthread_mutex_unlock(&this->evictionMutex);
                   return nullptr;
                }
                page->setPinned(true);
                page->incRefCount();
                this->evictionUnlock();
                pthread_mutex_unlock(&this->evictionMutex);
                pthread_mutex_lock(&this->countLock);
                page->setAccessSequenceId(this->accessCount);
                this->accessCount++;
                pthread_mutex_unlock(&this->countLock);
                if(set !=nullptr) {
                    set->updateCachedPage(page);
                }
	}

	return page;
}

//Below method is mainly to provide backward-compatibility for sequence files.
//note that below method will cause cached page reference count ++;
//NOT SUPPORTED ANY MORE, TO REMOVE THE METHOD
PDBPagePtr PageCache::getPage(SequenceFilePtr file, PageID pageId) {
	return nullptr;
}

//Below method will cause reference count ++;
//It will only be used in SetCachePageIterator class to get dirty pages, and will be guarded there
PDBPagePtr PageCache::getPage(CacheKey key, LocalitySet* set) {
       pthread_mutex_lock(&this->cacheMutex);
       if(this->containsPage(key) != true) {
           pthread_mutex_unlock(&this->cacheMutex);
           std :: cout << "WARNING: SetCachePageIterator get nullptr in cache.\n" << std :: endl;
           logger->warn("SetCachePageIterator get nullptr in cache.");
           return nullptr;
       } else {
           PDBPagePtr page = this->cache->at(key);
           pthread_mutex_unlock(&this->cacheMutex);
           if (page == nullptr) {
               std :: cout << "WARNING: SetCachePageIterator get nullptr in cache.\n" << std :: endl;
               logger->warn("SetCachePageIterator get nullptr in cache.");
               return nullptr;
           }
           page->incRefCount();
           pthread_mutex_lock(&this->countLock);
           page->setAccessSequenceId(this->accessCount);
           this->accessCount++;
           pthread_mutex_unlock(&this->countLock);
           if(set !=nullptr) {
               set->updateCachedPage(page);
           }
           return page;
       } 
}

PDBPagePtr PageCache::getNewPageNonBlocking(NodeID nodeId, CacheKey key, LocalitySet * set, size_t pageSize) {
       if(this->containsPage(key) == true) {
           return nullptr;
       }
       int internalOffset = 0;
       char * pageData;
       pageData = tryAllocateBufferFromSharedMemory(pageSize, internalOffset);
       if(pageData != nullptr) {
       } else {
            return nullptr;
       }
       PDBPagePtr page = make_shared<PDBPage>(pageData, nodeId, key.dbId,
        key.typeId, key.setId, key.pageId, pageSize, shm->computeOffset(pageData), internalOffset);

       pthread_mutex_lock(&this->countLock);
       page->setAccessSequenceId(this->accessCount);
       this->accessCount++;
       pthread_mutex_unlock(&this->countLock);
       page->setPinned(true);
       page->setDirty(true);
       pthread_mutex_lock(&evictionMutex);
       this->cachePage(page, set);
       page->incRefCount();
       pthread_mutex_unlock(&evictionMutex);
       return page;

}


//Assumption: for a new pageId, at one time, only one thread will try to allocate a new page for it
//To allocate a new page, set it as pinned&dirty, add it to cache, and increment reference count
PDBPagePtr PageCache::getNewPage(NodeID nodeId, CacheKey key, LocalitySet* set, size_t pageSize) {
       pthread_mutex_lock(&evictionMutex);
       if(this->containsPage(key) == true) {
           pthread_mutex_unlock(&evictionMutex);
           return nullptr;
       }
       pthread_mutex_unlock(&evictionMutex);
       int internalOffset = 0;
       char * pageData;
       pageData = allocateBufferFromSharedMemoryBlocking(pageSize, internalOffset);
       if(pageData != nullptr) {
            PDB_COUT << "PageCache: getNewPage: Page created for typeId=" <<key.typeId<<",setId="<<key.setId<<",pageId="<<key.pageId<<"\n";
       } else {
            PDB_COUT << "failed!!!\n";
            return nullptr;
       } 
       PDBPagePtr page = make_shared<PDBPage>(pageData, nodeId, key.dbId,
        key.typeId, key.setId, key.pageId, pageSize, shm->computeOffset(pageData), internalOffset);
      
       pthread_mutex_lock(&this->countLock);
       page->setAccessSequenceId(this->accessCount);
       this->accessCount++;
       pthread_mutex_unlock(&this->countLock);
       page->setPinned(true);
       page->setDirty(true);
       pthread_mutex_lock(&evictionMutex);
       this->cachePage(page, set);
       page->incRefCount();
       pthread_mutex_unlock(&evictionMutex);
       return page;
}

//please note that only below method will cause cached page reference count --

bool PageCache::decPageRefCount(CacheKey key) {
	if (this->containsPage(key) == false) {
		return false;
	} else {
		PDBPagePtr page = this->cache->at(key);
		page->decRefCount();
		return true;
	}
}

bool PageCache::containsPage(CacheKey key) {
	return (this->cache->find(key) != this->cache->end());
}



//Unpin all dirty pages
//IMPORTANT: This function can only be called before shutdown.
int PageCache::unpinAndEvictAllDirtyPages() {
    if (this->inEviction == true) {
           return 0;
    }
   
    this->logger->writeLn("Storage server: start cache eviction for all dirty pages...");
    pthread_mutex_lock(&this->evictionMutex);
    this->inEviction = true;
    int numEvicted = 0;
    PDBPagePtr page;
    unordered_map<CacheKey, PDBPagePtr, CacheKeyHash, CacheKeyEqual>::iterator cacheIter;
    vector<PDBPagePtr> * evictableDirtyPages = new vector<PDBPagePtr>();
    this->evictionLock();
    for (cacheIter = this->cache->begin(); cacheIter != this->cache->end(); cacheIter++) {
        page = cacheIter->second;
        if(page == nullptr) {
             this->inEviction = false;
             pthread_mutex_unlock(&this->evictionMutex);
             return 0;
        } else if
        ((page->isDirty() == true)&&(page->isInFlush()==false)) {
            while(page->getRefCount() > 0) {
                page->decRefCount();
            }
            evictableDirtyPages->push_back(page);
        } else {
            //do nothing
        }
    }
    this->evictionUnlock();
    int i;
    for (i = 0; i < evictableDirtyPages->size(); i++) {
        page = evictableDirtyPages->at(i);
        if(evictPage(page) == true) {
            numEvicted ++;
        }
    }

    this->inEviction = false;
    pthread_mutex_unlock(&this->evictionMutex);
    delete evictableDirtyPages;
    return numEvicted;
 

}



//Evict all dirty pages
int PageCache::evictAllDirtyPages() {
    if (this->inEviction == true) {
           return 0;
    }
    
    this->logger->writeLn("Storage server: start cache eviction for all dirty pages...");
    pthread_mutex_lock(&this->evictionMutex);
    this->inEviction = true;
    int numEvicted = 0;
    PDBPagePtr page;
    unordered_map<CacheKey, PDBPagePtr, CacheKeyHash, CacheKeyEqual>::iterator cacheIter;
    vector<PDBPagePtr> * evictableDirtyPages = new vector<PDBPagePtr>();
    this->evictionLock();
    for (cacheIter = this->cache->begin(); cacheIter != this->cache->end(); cacheIter++) {
        page = cacheIter->second;
        if(page == nullptr) {
             this->inEviction = false;
             pthread_mutex_unlock(&this->evictionMutex);
             return 0;
        } else if 
        ((page->isDirty() == true)&&(page->getRefCount()==0)&&(page->isInFlush()==false)) {
            evictableDirtyPages->push_back(page);
        } else {
            //do nothing
        }
    }
    this->evictionUnlock();
    int i;
    for (i = 0; i < evictableDirtyPages->size(); i++) {
        page = evictableDirtyPages->at(i);
        if(evictPage(page) == true) {
            numEvicted ++;
        }
    }

    this->inEviction = false;
    pthread_mutex_unlock(&this->evictionMutex);
    delete evictableDirtyPages;
    return numEvicted;
}



//Flush a page.
bool PageCache::flushPageWithoutEviction(CacheKey key) {
        if(this->containsPage(key) == true) {
            PDBPagePtr page = this->cache->at(key);
            if((page->isDirty() == true) && (page->isInFlush() == false)) {
                page->setInFlush(true);
                page->setInEviction(false);
                this->flushBuffer->addPageToTail(page);
            }
            else {
                //can't flush
                return false;
            }
        }
        else {
            //can't find page
            return false;
        }
        return true;
}


//Evict a page

bool PageCache::evictPage(CacheKey key, bool tryFlushOrNot) {
	if (this->containsPage(key) == true) {
		PDBPagePtr page = this->cache->at(key);
#ifndef UNPIN_FOR_NON_ZERO_REF_COUNT
		if (page->getRefCount() > 0) {
                        cout << "can't be unpinned due to non-zero reference count " << page->getRefCount()  << "with DatabaseID=" << page->getDbID() << ", TypeID=" << page->getTypeID() << ", SetID=" << page->getSetID() << ", PageID=" << page->getPageID() << "\n";
			this->logger->writeLn(
					"LRUPageCache: can not evict page because it has been pinned by at least one client");
			this->logger->writeInt(page->getPageID());
			return false;
		} else 
#endif
                      {
                        
                        page->setPinned(false);
                        if((tryFlushOrNot == true) && (page->isDirty() == true)&&(page->isInFlush()==false)&&((page->getDbID()!=0)||(page->getTypeID()!=1))&&((page->getDbID()!=0)||(page->getTypeID()!=2))) {
#ifdef PROFILING_CACHE
                            std :: cout << "going to unpin a dirty page...\n";
#endif
                            //update counter
                            //page->updateCounterInRawBytes(); 
                            page->setInFlush(true);
                            page->setInEviction(true);
                            //flush the page
                            //first we release the lock so that the flushing thread can run.
                            this->flushBuffer->addPageToTail(page);

                        }
                        else if /*((page->isDirty() == false) &&*/ (page->isInFlush()==false) {
#ifdef PROFILING_CACHE
                            std :: cout << "going to unpin a clean page...\n";
#endif
                            //free the page
                            //We use flush lock (which is a read write lock) to synchronize with getPage() that will be invoked in PartitionPageIterator;
                            //One scenario is: PDB load old data from disk to memory through iterators while application pins new pages that requires to evict data, then an old page in checking for loading may get evicted before it is pinned.
                            //Add flush lock is to guard for similar scenarios.
                            this->flushLock();
			    this->shm->free(page->getRawBytes()-page->getInternalOffset(), page->getRawSize()+512);
                            
			    page->setOffset(0);
			    page->setRawBytes(nullptr);
                            removePage(key);
                            this->flushUnlock();
                        }
#ifdef PROFILING_CACHE
                        std :: cout << "Storage server: evicting page from cache for dbId:" << page->getDbID() << ", typeID:"<<page->getTypeID()<<", setID="<<page->getSetID()<<", pageID: " << page->getPageID() << ", tryFlushing=" << tryFlushOrNot << ".\n";
#endif
		}

	} else {
                PDB_COUT << "can not find page in cache!\n";
		this->logger->writeLn(
				"LRUPageCache: can not evict page because it is not in cache");
		return false;
	}

	return true;
}

bool PageCache::evictPage(PDBPagePtr page, LocalitySetPtr set) {
	CacheKey key;
	key.dbId = page->getDbID();
	key.typeId = page->getTypeID();
	key.setId = page->getSetID();
	key.pageId = page->getPageID();
	bool ret= evictPage(key);
        if(ret == true) {
            if(set != nullptr) {
                set->removeCachedPage(page);
            }
        }
        return ret;
}

void PageCache::runEviction() {
	pdb :: PDBWorkerPtr worker;
	while ((worker = this->workers->getWorker()) == nullptr) {
		sleep(0);
	}
	PDBEvictWorkPtr evictWork = make_shared<PDBEvictWork>(this);
	PDBBuzzerPtr buzzer = evictWork->getLinkedBuzzer();
	worker->execute(evictWork, buzzer);
}

void PageCache::evict() {
	if (this->inEviction == true) {
		return;
	}
#ifdef PROFILING_CACHE
        std :: cout << "Storage server: starting cache eviction to get more room with used size= " << this->size << "!\n";
        //std :: cout << "current size is " << this->size << std :: endl;
#endif
        pthread_mutex_lock(&this->evictionMutex);
	this->inEviction = true;
        if(this->strategy == UnifiedIntelligent) {
            this->evictionLock();
            vector<PDBPagePtr> * pagesToEvict = nullptr;
            int i, j;
            int numEvicted = 0;
            list<LocalitySetPtr> * curList;
            for (i = 0; i < 6; i++) {
                curList = this->priorityList->at(i);
                for(list<LocalitySetPtr>::reverse_iterator it = curList->rbegin(); it != curList->rend(); ++it) {
                    LocalitySetPtr set = (*it);
                    pagesToEvict = set->selectPagesForReplacement();
                    if(pagesToEvict != nullptr) {
                        this->evictionUnlock();
                        for (j = 0; j < pagesToEvict->size(); j ++) {
                            if(this->evictPage(pagesToEvict->at(j), set)) {
                                numEvicted++;
                            }
                        }
                        this->evictionLock();
                        delete pagesToEvict;
                        pagesToEvict = nullptr;
                    }
                }
                if(numEvicted > 0) {
                    break;
                }
            }
            this->evictionUnlock();
            
        } else {
            this->evictionLock();
            this->logger->debug("PageCache::evict(): got the lock for evictionLock()...");
	    priority_queue<PDBPagePtr, vector<PDBPagePtr>, CompareCachedPagesMRU> * cachedPages =
			new priority_queue<PDBPagePtr, vector<PDBPagePtr>,
					CompareCachedPagesMRU>();
	    unordered_map<CacheKey, PDBPagePtr, CacheKeyHash, CacheKeyEqual>::iterator cacheIter;
            PDBPagePtr curPage;
	    for (cacheIter = this->cache->begin(); cacheIter != this->cache->end();
			cacheIter++) {
                curPage = cacheIter->second;
                if(curPage == nullptr) {
                      this->logger->error( "PageCache::evict(): got a null page, return!");
                      delete cachedPages;
                      this->inEviction = false;
                      this->evictionUnlock();
                      pthread_mutex_unlock(&this->evictionMutex);
                      return;
                }
                this->logger->debug( "PageCache::evict(): got a page, check whether it can be evicted...");
                if((curPage->getRefCount()==0)&&((curPage->isDirty()==false) || ((curPage->isDirty()==true)&&(curPage->isInFlush()==false)))){
		    cachedPages->push(curPage);
#ifdef PROFILING_CACHE
                    std :: cout << "Add to eviction queue: curPage->getRefCount()=" << curPage->getRefCount() << ", curPage->isDirty()=" << curPage->isDirty() << ", curPage->isInFlush)=" << curPage->isInFlush() << ", curPage->dbId=" << curPage->getDbID() << ", curPage->setId="<< curPage->getSetID() << std :: endl; 
#endif
                } else {
                    //do nothing
                }
	    }
            this->evictionUnlock();
	    PDBPagePtr page;
	    while ((this->size > this->evictStopSize)&&(cachedPages->size() > 0)) {
		page = cachedPages->top();
                if(page == nullptr) {
                    PDB_COUT << "PageCache: nothing to evict, return!\n";
                    this->logger->debug("PageCache: nothing to evict, return!\n");
                    break;
                }
		if (this->evictPage(page) == true) {
#ifdef PROFILING
			std :: cout << "Storage server: evicted page from cache passively for dbId:" << page->getDbID() << ", typeID:"<<page->getTypeID()<<", setID="<<page->getSetID()<<", pageID: " << page->getPageID() << ".\n";
#endif
		        this->logger->debug(std :: string("Storage server: evicting page from cache for pageID:")+std :: to_string(page->getPageID()));
			cachedPages->pop();
		} else {
                        cachedPages->pop();
                } 
	     }
	     delete cachedPages;
        }
	this->inEviction = false;
        pthread_mutex_unlock(&this->evictionMutex);
#ifdef PROFILING
        std :: cout << "Storage server: finished cache eviction!\n";
#endif
        logger->debug( "Storage server: finished cache eviction!\n");
}

void PageCache::getAndSetWarnSize(unsigned int numSets,
		double warnThreshold) {
	this->warnSize = (this->maxSize) * warnThreshold;
	this->logger->writeLn("LRUPageCache: warnSize was set to:");
	this->logger->writeInt(this->warnSize);
}

void PageCache::getAndSetEvictStopSize(unsigned int numSets,
		double evictThreshold) {
	this->evictStopSize = (this->maxSize)
			* evictThreshold;
	this->logger->writeLn("LRUPageCache: evictSize was set to:");
	this->logger->writeInt(this->evictStopSize);
}

void PageCache::addLocalitySetToPriorityList(LocalitySetPtr set, PriorityLevel level) {
        this->priorityList->at(level)->push_back(set);
}

void PageCache::removeLocalitySetFromPriorityList(LocalitySetPtr set, PriorityLevel level) {
        this->priorityList->at(level)->remove(set);
}

void PageCache::pin (LocalitySetPtr set, LocalitySetReplacementPolicy policy, OperationType operationType) {
        set->pin(policy, operationType);
        if(set->getPersistenceType() == Transient) {
            if(set->getLocalityType() == ShuffleData) {
                this->addLocalitySetToPriorityList(set, TransientLifetimeNotEndedShuffleData);
            } else if (set->getLocalityType() == HashPartitionData) {
                this->addLocalitySetToPriorityList(set, TransientLifetimeNotEndedHashData);
            } else {
                this->addLocalitySetToPriorityList(set, TransientLifetimeNotEndedPartialData);
            }
        }
        else {
            this->addLocalitySetToPriorityList(set, PersistentLifetimeNotEnded);
        }
}

void PageCache::unpin (LocalitySetPtr set) {
        set->unpin();
        if(set->getPersistenceType() == Transient) {
            if(set->getLocalityType() == ShuffleData) {
                this->removeLocalitySetFromPriorityList(set, TransientLifetimeNotEndedShuffleData);
            } else if (set->getLocalityType() == HashPartitionData) {
                this->removeLocalitySetFromPriorityList(set, TransientLifetimeNotEndedHashData);
            } else {
                this->removeLocalitySetFromPriorityList(set, TransientLifetimeNotEndedPartialData);
            }
        }
        else {
            this->removeLocalitySetFromPriorityList(set, PersistentLifetimeNotEnded);
        }
        if(set->getPersistenceType() == Transient) {
            this->addLocalitySetToPriorityList(set, TransientLifetimeEnded);
        }
        else {
            this->addLocalitySetToPriorityList(set, PersistentLifetimeEnded);
        }
}

#endif
