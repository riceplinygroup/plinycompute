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
 * File:   SetCachePageIterator.cc
 * Author: Jia
 *
 */


#ifndef SET_CACHE_PAGE_ITERATOR_CC
#define SET_CACHE_PAGE_ITERATOR_CC

#include "SetCachePageIterator.h"
//TODO using snapshot and reference count to support multiple concurrent iterators for one same buffer
//Argument: why do we need multiple concurrent iterators for the same buffer? 
//Maybe there are two concurrent jobs? so can we just share the iterator between two jobs?

//thread-safe (Fixed by Jia: now flushing is OK by updating the FileSearchKey)

//NOTE: the constructor can only be invoked in UserSet::getIterators(), where it will be protected by lockDirtyPageSet();

SetCachePageIterator::SetCachePageIterator(PageCachePtr cache, UserSet* set) {
    this->cache = cache;
    this->set =  set;
    //set->cleanDirtyPageSet();
    this->iter = this->set->getDirtyPageSet()->begin();
}

//remove all elements that have been flushed to disk (inCache == false)
SetCachePageIterator::~SetCachePageIterator() {
}

PDBPagePtr SetCachePageIterator::begin() {
    this->iter = this->set->getDirtyPageSet()->begin();
    return nullptr;
}

PDBPagePtr SetCachePageIterator::end() {
    this->iter = this->set->getDirtyPageSet()->end();
    return nullptr;
}

PDBPagePtr SetCachePageIterator::next() {
    //this->set->getLogger()->writeLn("SetCachePageIterator: to get lock for evictionLock()...");
    this->cache->evictionLock();
    //this->set->getLogger()->writeLn("SetCachePageIterator: got lock for evictionLock()...");
    if(this->iter != this->set->getDirtyPageSet()->end()) {
        if(this->iter->second.inCache == true) {
            CacheKey key;
            key.dbId = this->set->getDbID();
            key.typeId = this->set->getTypeID();
            key.setId = this->set->getSetID();
            key.pageId = this->iter->first;
            //this->set->getLogger()->writeLn("SetCachePageIterator: curPageId=");
            //this->set->getLogger()->writeInt(key.pageId);
            //cout << "SetCachePageIterator: curPageId="<<key.pageId<<"\n";
            PDBPagePtr page = this->cache->getPage(key, this->set);
            this->cache->evictionUnlock();
            //this->set->getLogger()->writeLn("SetCachePageIterator: unlocked for evictionLock()...");
            ++iter;
            return page;
        } else {
            this->cache->evictionUnlock();
            //this->set->getLogger()->writeLn("SetCachePageIterator: unlocked for evictionLock()...");
            //the page is already flushed to file, so load from file
            PageID pageId = this->iter->first;
            //this->set->getLogger()->writeLn("SetCachePageIterator: curPageId=");
            //this->set->getLogger()->writeInt(pageId);
            FileSearchKey searchKey = this->iter->second;
            PDBPagePtr page = this->cache->getPage(this->set->getFile(), searchKey.partitionId, searchKey.pageSeqInPartition, pageId, false, this->set);

            //remove iter
            //this->set->getLogger()->writeLn("SetCachePageIterator: to get lock for lockDirtyPageSet()...");
            this->set->lockDirtyPageSet();
            //this->set->getLogger()->writeLn("SetCachePageIterator: got lock for lockDirtyPageSet()...");
            this->iter = this->set->getDirtyPageSet()->erase(this->iter);
            this->set->unlockDirtyPageSet();
            //this->set->getLogger()->writeLn("SetCachePageIterator: unlock for lockDirtyPageSet()...");
            return page;           
        }
        
    }
    return nullptr;
}

bool SetCachePageIterator::hasNext() {
    if(this->iter != this->set->getDirtyPageSet()->end()) {
        return true;
    } else {
        return false;
    }
}

#endif
