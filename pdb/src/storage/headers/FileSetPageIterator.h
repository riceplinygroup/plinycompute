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

#ifndef FILESETPAGEITERATOR_H
#define FILESETPAGEITERATOR_H

#include "FileSet.h"
#include "DataTypes.h"
#include "PageCache.h"
#include "PageIterator.h"
#include <memory>
using namespace std;
class FileSetPageIterator;

typedef shared_ptr<FileSetPageIterator> FileSetPageIteratorPtr;

class FileSetPageIterator : public PageIteratorInterface {
public:
    FileSetPageIterator(PageCachePtr cache,
                        int fileHandle,
                        size_t fileSize,
                        size_t pageSize,
                        NodeID nodeId,
                        DatabaseID dbId,
                        UserTypeID typeId,
                        SetID setId);
    virtual ~FileSetPageIterator();
    PDBPagePtr next() override;
    bool hasNext() override;
    bool offsetIteratedSize(size_t iteratedSize);

private:
    PageCachePtr cache;
    int handle;
    size_t fileSize;
    size_t pageSize;
    size_t iteratedSize;
    NodeID nodeId;
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
    PageID pageId;
};

#endif
