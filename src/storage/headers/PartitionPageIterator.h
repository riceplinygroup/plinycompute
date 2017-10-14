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
 * PartitionPageIterator.h
 *
 *  Created on: Dec 25, 2015
 *      Author: Jia
 */

#ifndef SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONPAGEITERATOR_H_
#define SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONPAGEITERATOR_H_

#include "PageIterator.h"
#include "PDBFile.h"
#include "PageCache.h"
#include "UserSet.h"

class PartitionPageIterator : public PageIteratorInterface {

public:
    /**
     * To create a new PartitionPageIterator instance
     */
    PartitionPageIterator(PageCachePtr cache,
                          PDBFilePtr file,
                          FilePartitionID partitionId,
                          UserSet* set = nullptr);
    /*
     * To support polymorphism.
     */
    ~PartitionPageIterator(){};

    /**
     * To return the next page. If there is no more page, return nullptr.
     */
    PDBPagePtr next();

    /**
     * If there is more page, return true, otherwise return false.
     */
    bool hasNext();

private:
    PageCachePtr cache;
    PDBFilePtr file;
    FileType type;
    PartitionedFilePtr partitionedFile;
    SequenceFilePtr sequenceFile;
    FilePartitionID partitionId;
    unsigned int numPages;
    unsigned int numIteratedPages;
    UserSet* set;
};


#endif /* SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONPAGEITERATOR_H_ */
