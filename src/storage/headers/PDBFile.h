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
 * File:   PDBFile.h
 * Author: Jia
 *
 * Created on October 19, 2015, 4:51 PM
 */

#ifndef PDBFILE_H
#define PDBFILE_H

#include <stdio.h>
#include <vector>
#include "DataTypes.h"
#include "PDBPage.h"
#include <memory>
using namespace std;

class PDBFileInterface;
typedef shared_ptr<PDBFileInterface> PDBFilePtr;

/**
 * This class wraps an interface to implement
 * different kinds of disk file formats for data persistence.
 */
class PDBFileInterface {
public:
    /*
     * To support polymorphism.
     */
    virtual ~PDBFileInterface() {}

    /**
     * To open a file.
     */
    virtual bool openAll() = 0;

    /**
     * To close a file.
     */
    virtual bool closeAll() = 0;

    /**
     * To delete a file.
     */
    virtual void clear() = 0;

    /**
     * To append page to a file.
     */
    virtual int appendPage(FilePartitionID partitionId, PDBPagePtr page) = 0;

    /**
     * To initialize meta data of a file.
     */
    virtual int writeMeta() = 0;

    /**
     * To update meta data of a file
     */
    virtual int updateMeta() = 0;

    /**
     * To load a page from the file partition to cache.
     * If the file is of SequenceFile type, set the partitionId = 0;
     */
    virtual size_t loadPage(FilePartitionID partitionId,
                            unsigned int pageSeqInPartition,
                            char* pageInCache,
                            size_t length) = 0;


    /**
     * To set and return total number of flushed pages;
     */
    virtual unsigned int getAndSetNumFlushedPages() = 0;

    /**
     * To return total number of flushed pages.
     */
    virtual unsigned int getNumFlushedPages() = 0;

    /**
     * To return NodeID associated with the file.
     */
    virtual NodeID getNodeId() = 0;

    /**
     * To return DatabaseID associated with the file.
     */
    virtual DatabaseID getDbId() = 0;

    /**
     * To return UserTypeID associated with the file.
     */
    virtual UserTypeID getTypeId() = 0;

    /**
     * To return SetID associated with the file.
     */
    virtual SetID getSetId() = 0;

    /**
     * To return the PageID of last page flushed to the file.
     */
    virtual PageID getLastFlushedPageID() = 0;

    /**
     * To return the PageID of latest page.
     */
    virtual PageID getLatestPageID() = 0;

    /**
     * To return page size of the file
     */
    virtual size_t getPageSize() = 0;

    /**
     * To return page size of the file by looking into the meta data.
     */
    virtual size_t getPageSizeInMeta() = 0;

    /**
     * To return the file type of the file: SequenceFile or PartitionedFile
     */
    virtual FileType getFileType() = 0;
};


#endif /* PDBFILE_H */
