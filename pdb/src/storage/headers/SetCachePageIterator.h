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

#ifndef SETCACHEPAGEITERATOR_H
#define SETCACHEPAGEITERATOR_H


#include "UserSet.h"
#include "DataTypes.h"
#include "PageCache.h"
#include "PageIterator.h"
#include <set>
#include <memory>
using namespace std;
class SetCachePageIterator;

typedef shared_ptr<SetCachePageIterator> SetCachePageIteratorPtr;

/**
 * This class implements an iterator to all dirty pages stored in PageCache that hasn't been flushed
 * ever for a given set.
 */
class SetCachePageIterator : public PageIteratorInterface {
public:
    // NOTE: the constructor can only be invoked in UserSet::getIterators(), where it will be
    // protected by lockDirtyPageSet();
    SetCachePageIterator(PageCachePtr cache, UserSet* set);
    virtual ~SetCachePageIterator();

    /**
     * Returns the page with the smallest PageID in the input buffer.
     */
    PDBPagePtr begin();

    /**
     * Returns the page with the largest PageID in the input buffer.
     */
    PDBPagePtr end();

    /**
     * Returns the next page in the input buffer.
     * If there is no more page, returns nullptr.
     */
    PDBPagePtr next() override;

    /**
     * If there is more pages in the input buffer, return true, else, return false.
     */
    bool hasNext() override;

private:
    PageCachePtr cache;
    UserSet* set;
    std::unordered_map<PageID, FileSearchKey>::iterator iter;
};


#endif /* SETCACHEPAGEITERATOR_H */
