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
 * PartitionedFileMetaData.h
 *
 *  Created on: Dec 20, 2015
 *      Author: Jia
 */

#ifndef SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONEDFILEMETADATA_H_
#define SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONEDFILEMETADATA_H_

#include "DataTypes.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <pthread.h>
using namespace std;

class PartitionedFileMetaData;
typedef shared_ptr<PartitionedFileMetaData> PartitionedFileMetaDataPtr;

class PartitionMetaData;
typedef shared_ptr<PartitionMetaData> PartitionMetaDataPtr;

struct PageIndexHash {
    std::size_t operator()(const PageIndex& index) const {
        return (index.partitionId << 16) + index.pageSeqInPartition;
    }
};

struct PageIndexEqual {
    bool operator()(const PageIndex& lIndex, const PageIndex& rIndex) const {
        if (lIndex.partitionId != rIndex.partitionId) {
            return false;
        } else {
            if (lIndex.pageSeqInPartition != rIndex.pageSeqInPartition) {
                return false;
            } else {
                return true;
            }
        }
    }
};

/**
 * This class wraps the meta data format for PartitionedFile instance.
 */
class PartitionedFileMetaData {
public:
    // Create an empty PartitionedFileMetaData instance.
    PartitionedFileMetaData() {
        this->version = 0;
        this->partitions = nullptr;
        this->pageSize = 0;
        this->numFlushedPages = 0;
        this->pageIndexes = new unordered_map<PageID, PageIndex>();
        this->pageIds = new unordered_map<PageIndex, PageID, PageIndexHash, PageIndexEqual>();
        pthread_mutex_init(&(this->metaMutex), nullptr);
        pthread_mutex_init(&(this->indexMutex), nullptr);
    }

    // Destructor, it will NOT delete the meta partition or any other file partitions.
    ~PartitionedFileMetaData() {
        if (partitions != nullptr) {
            partitions->clear();
            delete partitions;
        }
        if (pageIndexes != nullptr) {
            pageIndexes->clear();
            delete pageIndexes;
        }
        if (pageIds != nullptr) {
            pageIds->clear();
            delete pageIds;
        }
    }

    // Return total number of flushed pages in all data partitions of this PartitionedFile instance
    unsigned int getNumFlushedPages() const {
        return this->numFlushedPages;
    }

    // Set total number of flushed pages in all data partitions of this PartitionedFile instance
    void setNumFlushedPages(unsigned int numFlushedPages) {
        this->numFlushedPages = numFlushedPages;
    }

    void addPageIndex(PageID pageId, FilePartitionID partitionId, unsigned int pageSeqInPartition) {
        pthread_mutex_lock(&indexMutex);
        PageIndex pageIndex;
        pageIndex.partitionId = partitionId;
        pageIndex.pageSeqInPartition = pageSeqInPartition;
        this->pageIndexes->insert(pair<PageID, PageIndex>(pageId, pageIndex));
        this->pageIds->insert(pair<PageIndex, PageID>(pageIndex, pageId));
        pthread_mutex_unlock(&indexMutex);
    }

    PageIndex getPageIndex(PageID pageId) {

        PageIndex pageIndex;
        if (this->pageIndexes->find(pageId) == this->pageIndexes->end()) {
            pageIndex.partitionId = (unsigned int)(-1);
            pageIndex.pageSeqInPartition = (unsigned int)(-1);
        } else {
            pageIndex = this->pageIndexes->at(pageId);
        }
        return pageIndex;
    }

    PageID getPageId(FilePartitionID partitionId, unsigned int pageSeqInPartition) {
        PageIndex pageIndex;
        pageIndex.partitionId = partitionId;
        pageIndex.pageSeqInPartition = pageSeqInPartition;
        PageID pageId;
        if (this->pageIds->find(pageIndex) == this->pageIds->end()) {
            pageId = (unsigned int)(-1);
        } else {
            pageId = this->pageIds->at(pageIndex);
        }
        return pageId;
    }

    // Increment total number of flushed pages in all data partitions of this PartitionedFile
    // instance
    void incNumFlushedPages() {
        pthread_mutex_lock(&metaMutex);
        this->numFlushedPages++;
        pthread_mutex_unlock(&metaMutex);
    }

    // Return the page size
    size_t getPageSize() const {
        return pageSize;
    }

    // Return the latest pageId that has been allocated for this set
    PageID getLatestPageId() const {
        return latestPageId;
    }

    // Set the page size
    void setPageSize(size_t pageSize) {
        this->pageSize = pageSize;
    }

    // Return all partitions
    vector<PartitionMetaDataPtr>* getPartitions() const {
        return partitions;
    }

    // Return partition specified by Id
    PartitionMetaDataPtr getPartition(FilePartitionID partitionId) const {
        return partitions->at(partitionId);
    }

    // Set all partitions
    void setPartitions(vector<PartitionMetaDataPtr>* partitions = nullptr) {
        this->partitions = partitions;
    }

    // Add a new partition
    void addPartition(PartitionMetaDataPtr partition) {
        if (this->partitions == nullptr) {
            this->partitions = new vector<PartitionMetaDataPtr>();
        }
        partitions->push_back(partition);
    }

    // Return current metadata version
    unsigned short getVersion() const {
        return version;
    }

    // Set current metadata version
    void setVersion(unsigned short version) {
        this->version = version;
    }


    // Set latest pageId
    void setLatestPageId(PageID pageId) {
        this->latestPageId = pageId;
    }

    unordered_map<PageID, PageIndex>* getPageIndexes() {
        return pageIndexes;
    }

private:
    // Metadata version
    unsigned short version;
    // A vector of partition meta data
    vector<PartitionMetaDataPtr>* partitions = nullptr;
    // Page size
    size_t pageSize;
    // Number of flushed pages
    unsigned int numFlushedPages;
    // Last PageID (The largest pageId that has been allocated for this set)
    PageID latestPageId;
    // a map of PageID to PageIndex
    unordered_map<PageID, PageIndex>* pageIndexes = nullptr;
    unordered_map<PageIndex, PageID, PageIndexHash, PageIndexEqual>* pageIds = nullptr;
    pthread_mutex_t metaMutex;
    pthread_mutex_t indexMutex;
};

/**
 * This class wraps the meta data format for a Partitioned instance.
 */
class PartitionMetaData {
public:
    // Create an empty PartitionMetaData instance
    PartitionMetaData() {}

    // Create a new PartitionMetaData instance
    PartitionMetaData(string path, FilePartitionID partitionId) {
        this->numPages = 0;
        this->path = path;
        this->partitionId = partitionId;
    }

    ~PartitionMetaData() {}

    /**
     * Return number of pages in the partition
     */
    unsigned int getNumPages() const {
        return numPages;
    }

    /**
     * Set number of pages in the partition
     */
    void setNumPages(unsigned int numPages) {
        this->numPages = numPages;
    }

    /**
     * Increment number of pages in the partition
     */
    void incNumPages() {
        this->numPages++;
    }

    /**
     * Return the PartitionID of this partition
     */
    FilePartitionID getPartitionId() const {
        return partitionId;
    }

    /**
     * Set the PartitionID for this partition
     */
    void setPartitionId(FilePartitionID partitionId) {
        this->partitionId = partitionId;
    }

    /**
     * Return the path to this partition
     */
    string getPath() const {
        return path;
    }

    /**
     * Set the path for this partition
     */
    void setPath(string path) {
        this->path = path;
    }

private:
    /**
     * PartitionID of this partition
     */
    FilePartitionID partitionId;

    /**
     * Number of pages in the partition
     */
    unsigned int numPages;

    /**
     * Path of this partition;
     */
    string path;
};

#endif /* SRC_CPP_MAIN_DATABASE_HEADERS_PARTITIONEDFILEMETADATA_H_ */
