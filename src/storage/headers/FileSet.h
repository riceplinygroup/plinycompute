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
 * FileSet.h
 *
 * Created on: May 26, 2016
 * Author: Jia
 */


#ifndef FILESET_H
#define FILESET_H

#include "PageCache.h"
#include "PDBLogger.h"
#include "PageIterator.h"
#include "PageCache.h"
#include "DataTypes.h"
#include <memory>
#include <string>
using namespace std;

class FileSet;
typedef shared_ptr<FileSet> FileSetPtr;

class FileSet : public LocalitySet {
public:
    FileSet(PageCachePtr cache,
            string filePath,
            size_t pageSize,
            NodeID nodeId,
            DatabaseID dbId,
            UserTypeID typeId,
            SetID setId);
    ~FileSet();
    void clear();
    int writeData(void* data, size_t length);
    PageIteratorPtr getIterator();
    NodeID getNodeID() {
        return this->nodeId;
    }

    DatabaseID getDatabaseID() {
        return this->dbId;
    }
    UserTypeID getUserTypeID() {
        return this->typeId;
    }
    SetID getSetID() {
        return this->setId;
    }
    string getFilePath() {
        return this->filePath;
    }

private:
    string filePath;
    int handle;
    PageCachePtr cache;
    size_t pageSize;
    size_t fileSize;
    NodeID nodeId;
    DatabaseID dbId;
    UserTypeID typeId;
    SetID setId;
};

#endif
