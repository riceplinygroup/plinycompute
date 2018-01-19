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

#include "PDBDebug.h"
#include "PartitionPageIterator.h"

/**
 * To create a new PartitionPageIterator instance
 */
PartitionPageIterator::PartitionPageIterator(PageCachePtr cache,
                                             PDBFilePtr file,
                                             FilePartitionID partitionId,
                                             UserSet* set) {
    this->cache = cache;
    this->file = file;
    this->partitionId = partitionId;
    this->set = set;
    if ((this->type = file->getFileType()) == FileType::SequenceFileType) {
        this->sequenceFile = dynamic_pointer_cast<SequenceFile>(file);
        this->partitionedFile = nullptr;
        this->numPages = file->getNumFlushedPages();
    } else {
        this->sequenceFile = nullptr;
        this->partitionedFile = dynamic_pointer_cast<PartitionedFile>(file);
        this->numPages = partitionedFile->getMetaData()->getPartition(partitionId)->getNumPages();
    }
    this->numIteratedPages = 0;
}

/**
 * To return the next page. If there is no more page, return nullptr.
 */
PDBPagePtr PartitionPageIterator::next() {
    PDBPagePtr pageToReturn;
    if (this->numIteratedPages >= this->numPages) {
        return nullptr;
    } else {
        if (this->type == FileType::SequenceFileType) {
            pageToReturn = cache->getPage(this->sequenceFile, this->numIteratedPages);
            this->numIteratedPages++;
        } else {
            PageID curPageId =
                this->partitionedFile->loadPageId(this->partitionId, this->numIteratedPages);
            PDB_COUT << "PartitionedPageIterator: curTypeId=" << this->partitionedFile->getTypeId()
                     << ",curSetId=" << this->partitionedFile->getSetId()
                     << ",curPageId=" << curPageId << "\n";
// page is pinned (ref count ++)
#ifdef USE_LOCALITY_SET
            pageToReturn = cache->getPage(this->partitionedFile,
                                          this->partitionId,
                                          this->numIteratedPages,
                                          curPageId,
                                          false,
                                          set);
#else
            pageToReturn = cache->getPage(this->partitionedFile,
                                          this->partitionId,
                                          this->numIteratedPages,
                                          curPageId,
                                          false,
                                          nullptr);
#endif
            PDB_COUT << "PartitionedPageIterator: got page" << std::endl;
            this->numIteratedPages++;
        }
    }
    return pageToReturn;
}

/**
 * If there is more page, return true, otherwise return false.
 */
bool PartitionPageIterator::hasNext() {
    if (this->numIteratedPages < this->numPages) {
        return true;
    } else {
        return false;
    }
}
