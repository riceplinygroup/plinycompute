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
 * File:   SequenceFile.h
 * Author: Jia
 *
 * Created on October 20, 2015, 7:32 PM
 */

#ifndef SEQUENCEFILE_H
#define	SEQUENCEFILE_H

#include "PDBFile.h"
#include "DataTypes.h"
#include <string>
#include <memory>
#include "PDBLogger.h"

using namespace std;
class SequenceFile;
typedef shared_ptr<SequenceFile> SequenceFilePtr;

/**
 * This class wraps a SequenceFile class that implements the PDBFileInterface.
 * If using SequenceFile, each set will be flushed to a single file.
 */
class SequenceFile: public PDBFileInterface {
public:
	SequenceFile(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
			string path, pdb::PDBLoggerPtr logger, size_t pageSize);
	~SequenceFile();

	bool openAll() override;
	bool closeAll() override;
	void clear() override;
	int appendPage(FilePartitionID partitionId, PDBPagePtr page) override;
	int appendPage(PDBPagePtr page);
	int writeMeta() override;
	int updateMeta() override;

	/**
	 *
	 * To load data of given length on the specified page to cache.
	 * This function will be invoked by PageCache instance,
	 * length is the size of shared memory allocated for this load.
	 * The PageCache instance should make sure that length == pageSize.
	 * Otherwise, if length > pageSize, some space will be wasted, and
	 * if length < pageSize, some data on the page may not get loaded.
	 *
	 */
	size_t loadPage(PageID pageId, char * pageInCache, size_t length);

	/**
	 * To load a page from the file partition to cache memory.
	 * Because this is the SequenceFile type, the parameter partitionId can be ignored,
	 * and pageSeqInPartition in a SequenceFile instance is always equivalent to PageID;
	 */
	size_t loadPage(FilePartitionID partitionId,
			unsigned int pageSeqInPartition, char * pageInCache, size_t length)
					override;

	unsigned int getAndSetNumFlushedPages() override;
	unsigned int getNumFlushedPages() override;
	PageID getLastFlushedPageID() override;
        PageID getLatestPageID() override;
        size_t getPageSize() override;
	size_t getPageSizeInMeta() override;

    /**
     * To return the file type of the file: SequenceFile or PartitionedFile
     */
    FileType getFileType() override {
    	return FileType::SequenceFileType;
    }

	DatabaseID getDbId() override {
		return dbId;
	}

	NodeID getNodeId() override {
		return nodeId;
	}

	SetID getSetId() override {
		return setId;
	}

	UserTypeID getTypeId() override {
		return typeId;
	}

	/**
	 * To return path of the file.
	 */
	string getPath() {
		return filePath;
	}


protected:
	/**
	 * Seek to the page size field in meta data.
	 */
	int seekPageSizeInMeta();

	/**
	 * Seek to the last flushed pageId field in meta data.
	 */
	int seekLastFlushedPageID();

	/**
	 * Seek to the beginning of a page specified in the file.
	 */
	int seekPage(PageID pageId);

	/**
	 * Write data specified to the current file position.
	 */
	int writeData(void * data, size_t length);

	/**
	 * Seek to the end of the file.
	 */
	int seekEnd();

private:
	NodeID nodeId;
	DatabaseID dbId;
	UserTypeID typeId;
	SetID setId;
	FILE * file;
	string filePath;
	PageID lastFlushedId;
	unsigned int numFlushedPages;
	size_t pageSize = 0;
	size_t metaSize;
	pdb::PDBLoggerPtr logger;
};

#endif	/* SEQUENCEFILE_H */

