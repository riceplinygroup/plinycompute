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

#ifndef PAGECACHE_H
#define PAGECACHE_H


#include "Configuration.h"
#include "PDBWork.h"
#include "PDBWorkerQueue.h"
#include "PDBWorker.h"
#include "PDBFile.h"
#include "SequenceFile.h"
#include "PartitionedFile.h"
#include "PDBLogger.h"
#include "SharedMem.h"
#include "PageCircularBuffer.h"
#include "LocalitySet.h"
#include <unordered_map>
#include <memory>
#include <queue>
using namespace std;

class PageCache;
typedef shared_ptr<PageCache> PageCachePtr;

/**
 * How many cache policies we want to support?
 *
 * 0. LRU.
 * 1. MRU.
 * 2. Priority-based LRU/MRU.
 *    Step 1. select Set based on following priority order
 *    - Lifetime-ended task-level visibility data
 *    - Lifetime-ended job-level visibility data
 *    - Lifetime-ended application-level visibility data
 *    - To-expire application-level visibility data
 *    - To-expire job-level visibility data
 *    - To-expire task-level visibility data
 *    Step 2. evict one or more pages from the Set based on LRU/MRU
 * 3. Cost-based.
 */


/**
 * Hash function for CacheKey, used for caching and retrieving a page.
 */

struct CacheKeyHash {

    std::size_t operator()(const CacheKey& key) const {
        return (key.dbId << 24) + (key.typeId << 16) + (key.setId << 8) + key.pageId;
    }
};

/**
 * Comparator for CacheKey, used for caching and retrieving a page.
 */

struct CacheKeyEqual {

    bool operator()(const CacheKey& lKey, const CacheKey& rKey) const {
        if ((lKey.dbId == rKey.dbId) && (lKey.typeId == rKey.typeId) &&
            (lKey.setId == rKey.setId) && (lKey.pageId == rKey.pageId)) {
            return true;
        } else {
            return false;
        }
    }
};

/**
 * Comparator for the last access time of two cached pages, used for eviction.
 */
struct CompareCachedPages {

    bool operator()(PDBPagePtr& lPage, PDBPagePtr& rPage) {
        if (lPage->getAccessSequenceId() < rPage->getAccessSequenceId()) {
            return false;
        } else {
            return true;
        }
    }
};

/**
 * Comparator for the last access time of two cached pages, used for eviction.
 */
struct CompareCachedPagesMRU {

    bool operator()(PDBPagePtr& lPage, PDBPagePtr& rPage) {
        if (lPage->getAccessSequenceId() > rPage->getAccessSequenceId()) {
            return false;
        } else {
            return true;
        }
    }
};

/**
 * This class wraps a global page cache adopting multiple eviction policy, and by default it uses
 * MRU.
 * Each slave node will have one page cache that
 * can be accessed by frontend and forked backend via shared memory.
 *
 * Typical scenario for scanning data
 * Step 1. Frontend will load pages to cache by scanning partitions and input buffer
 * Step 2. For each loaded page, frontend will pin it, and tell backend about the location of the
 * pinned page
 * Step 3. Backend will tell frontend to unpin a page
 *
 * Typical scenario for output data
 * Step 1. Backend will tell frontend to create a temp set or a user set, depending on output data's
 * lifetime and visibility;
 * Step 2. Backend will tell frontend to add a new page and pin it as dirty;
 * Step 3. Backend will tell frontend to unpin the page;
 * Step 4. Backend will tell frontend to pin the page again;(at this time, page may be flushed to
 * disk file, and needs to load it to cache first)
 * Step 5. Backend will tell frontend to unpin the page;
 * Step 6. Backend will tell frontend to remove a temp set.
 */
class PageCache {

public:
    // create an MRU page cache
    PageCache(ConfigurationPtr conf,
              pdb::PDBWorkerQueuePtr workers,
              PageCircularBufferPtr flushBuffer,
              pdb::PDBLoggerPtr logger,
              SharedMemPtr shm,
              CacheStrategy strategy = UnifiedMRU);
    ~PageCache();

    // Get a page from cache, if the page is flushed to file, and is not in cache,
    // load it to cache. Otherwise, if the page is not in cache, and can not be found in any flushed
    // file,
    // we return nullptr.
    // Below method will cause page reference count ++
    // This function is used to provide backward-compatibility for SequenceFile instances, and
    // can only be applied to SequenceFile instances.
    PDBPagePtr getPage(SequenceFilePtr file, PageID pageId);


    // Get a page from cache, if the page is flushed to file, and is not in cache, load it to cache.
    // Otherwise, if the page is not in cache, and can not be found in any flushed file, we return
    // nullptr.
    // Below method will cause page reference count ++
    // This function can be used for frontend to pin a flushed page.
    // This function can also be used for backend to pin a flushed page that has been pinned and
    // unpinned before by the same backend.
    // It can be applied to all PDBFile instances.
    // If sequential==true, we will invoke file's sequential read API that will not `seek` first.
    PDBPagePtr getPage(PartitionedFilePtr file,
                       FilePartitionID partitionId,
                       unsigned int pageSeqInPartition,
                       PageID pageId,
                       bool sequential,
                       LocalitySet* set = nullptr);


    // Get a page directly from cache, if it is not in cache return nullptr
    PDBPagePtr getPage(CacheKey key, LocalitySet* set = nullptr);


    // To allocate a new page, blocking until get a page, set it as pinned&dirty, add it to cache,
    // and increment reference count
    PDBPagePtr getNewPage(NodeID nodeId,
                          CacheKey key,
                          LocalitySet* set = nullptr,
                          size_t pageSize = DEFAULT_PAGE_SIZE);

    // Try to allocate a new page, set it as pinned&dirty, add it to cache, and increment reference
    // count
    PDBPagePtr getNewPageNonBlocking(NodeID nodeId,
                                     CacheKey key,
                                     LocalitySet* set = nullptr,
                                     size_t pageSize = DEFAULT_PAGE_SIZE);

    // Decrement reference count for a page.
    // In the LRUPageCache class, only below method will cause page reference count --
    bool decPageRefCount(CacheKey key);

    // If the page specified by the cache key is in cache, return true,
    // otherwise, return false.
    bool containsPage(CacheKey key);


    // Evict all dirty pages
    int evictAllDirtyPages();

    // Unpin and evict all dirty pages
    int unpinAndEvictAllDirtyPages();

    // Start the eviction thread to evict least recently used pages.
    void runEviction();

    // Invoke the eviction in a method instead of a separate thread.
    void evict();

    // Evict page specified by cachekey from cache.
    bool evictPage(CacheKey key, bool tryFlushOrNot = true);

    // Compute the threshold when to trigger eviction.
    void getAndSetWarnSize(unsigned int numSets, double warnThreshold);

    // Compute the threshold when eviction can be finished.
    void getAndSetEvictStopSize(unsigned int numSets, double evictThreshold);

    // Load page specified from disk file to cache.
    // This function can only be applied to SequenceFile instances.
    PDBPagePtr loadPage(SequenceFilePtr file, PageID pageId);

    // Load page specified from disk file to cache.
    // This function can be applied to all PDBFile instances.
    // If sequential=true, we will invoke file's sequential read API if the file instance has
    // provided such API.
    PDBPagePtr loadPage(PDBFilePtr file,
                        FilePartitionID partitionId,
                        unsigned int pageSeqInPartition,
                        bool sequential);

    // Remove page specified by Key from cache hashMap.
    // This function will be used by the flushConsumer thread.
    bool removePage(CacheKey key);
    bool freePage(PDBPagePtr page);
    // Lock for eviction.
    void evictionLock();

    // Unlock for eviction.
    void evictionUnlock();

    // Lock for flushing.
    void flushLock();

    // Unlock for flushing.
    void flushUnlock();

    // Flush a page.
    bool flushPageWithoutEviction(CacheKey key);

    // Allocate buffer of required size from shared memory, if no room, block and run eviction
    // thread.
    char* allocateBufferFromSharedMemoryBlocking(size_t size, int& alignOffset);

    // TODO: Allocate buffer of required size from shared memory, if no room, block and evict only
    // one page.
    char* tryAllocateBufferFromSharedMemory(size_t size, int& alignOffset);

    PDBPagePtr buildAndCachePageFromFileHandle(int handle,
                                               size_t size,
                                               NodeID nodeId,
                                               DatabaseID dbId,
                                               UserTypeID typeId,
                                               SetID setId,
                                               PageID pageId);


    // Build a PDBPage instance from page data loaded from file into shared memory.
    PDBPagePtr buildPageFromSharedMemoryData(PDBFilePtr file,
                                             char* pageData,
                                             FilePartitionID partitionId,
                                             unsigned int pageSeqInPartition,
                                             int internalOffset,
                                             size_t pageSize = DEFAULT_PAGE_SIZE);


    // Cache the block with specified name and buffer.
    void cachePage(PDBPagePtr page, LocalitySet* set = nullptr);

    // Evict page from cache.
    bool evictPage(PDBPagePtr page, LocalitySetPtr set = nullptr);

    void addLocalitySetToPriorityList(LocalitySetPtr set, PriorityLevel level);

    void removeLocalitySetFromPriorityList(LocalitySetPtr set, PriorityLevel level);


    // Get logger
    pdb::PDBLoggerPtr getLogger() {
        return this->logger;
    }

    void pin(LocalitySetPtr set, LocalitySetReplacementPolicy policy, OperationType operationType);

    void unpin(LocalitySetPtr set);


private:
    unordered_map<CacheKey, PDBPagePtr, CacheKeyHash, CacheKeyEqual>* cache;
    pdb::PDBLoggerPtr logger;
    ConfigurationPtr conf;
    unsigned int size;
    size_t maxSize;
    size_t warnSize;       // the threshold to evict
    size_t evictStopSize;  // the threshold to stop eviction
    pthread_rwlock_t evictionAndFlushLock;
    pthread_mutex_t cacheMutex;
    pthread_mutex_t evictionMutex;
    bool inEviction;
    pdb::PDBWorkerQueuePtr workers;
    pdb::PDBWorkPtr evictWork;
    long accessCount;
    pthread_mutex_t countLock;
    SharedMemPtr shm;
    PageCircularBufferPtr flushBuffer;
    CacheStrategy strategy;
    /*
     * index = 0, TransientLifetimeEnded
     * index = 1, PersistentLifetimeEnded
     * index = 2, PersistentLifetimeNotEnded
     * index = 3, TransientLifetimeNotEndedPartialData
     * index = 4, TransientLifetimeNotEndedShuffleData
     * index = 5, TransientLifetimeNotEndedHashData
     */
    vector<list<LocalitySetPtr>*>* priorityList;
};
#endif /* PAGECACHE_H */
