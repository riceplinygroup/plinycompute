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

#ifndef FILESETPAGEITERATOR_CC
#define FILESETPAGEITERATOR_CC

#include "FileSetPageIterator.h"
#include <stdio.h>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>

FileSetPageIterator::FileSetPageIterator(PageCachePtr cache,
                                         int handle,
                                         size_t fileSize,
                                         size_t pageSize,
                                         NodeID nodeId,
                                         DatabaseID dbId,
                                         UserTypeID typeId,
                                         SetID setId) {
    this->cache = cache;
    this->handle = handle;
    this->fileSize = fileSize;
    this->pageSize = pageSize;
    this->iteratedSize = 0;
    // Suppose there is only one iterator running at a time
    lseek(handle, 0, SEEK_SET);
    this->nodeId = nodeId;
    this->dbId = dbId;
    this->typeId = typeId;
    this->setId = setId;
    this->pageId = 0;
}

FileSetPageIterator::~FileSetPageIterator() {}

bool FileSetPageIterator::hasNext() {
    if (iteratedSize < this->fileSize) {
        return true;
    } else {
        return false;
    }
}


PDBPagePtr FileSetPageIterator::next() {
    if (iteratedSize >= this->fileSize) {
        return nullptr;
    } else {
        size_t size = this->fileSize - iteratedSize;
        PDBPagePtr page;
        if (size < pageSize) {
            page = cache->buildAndCachePageFromFileHandle(
                handle, size, nodeId, dbId, typeId, setId, pageId);
            this->iteratedSize += size;
        } else {
            page = cache->buildAndCachePageFromFileHandle(
                handle, pageSize, nodeId, dbId, typeId, setId, pageId);
            this->iteratedSize += pageSize;
        }

        pageId++;
        return page;
    }
}

bool FileSetPageIterator::offsetIteratedSize(size_t offset) {
    if (offset < this->iteratedSize) {
        this->iteratedSize = this->iteratedSize - offset;
        return true;
    } else {
        return false;
    }
}

#endif
