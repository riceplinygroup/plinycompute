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

#include "PartitionedFile.h"
#include <stdio.h>
#include <vector>
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
#include <chrono>
#include <ctime>
using namespace std;
/**
 * Create a new PartitionedFile instance.
 */
PartitionedFile::PartitionedFile(NodeID nodeId,
                                 DatabaseID dbId,
                                 UserTypeID typeId,
                                 SetID setId,
                                 string metaPartitionPath,
                                 vector<string> dataPartitionPaths,
                                 pdb::PDBLoggerPtr logger,
                                 size_t pageSize) {
    unsigned int i = 0;

    this->nodeId = nodeId;
    this->dbId = dbId;
    this->typeId = typeId;
    this->setId = setId;
    this->metaPartitionPath = metaPartitionPath;
    this->dataPartitionPaths = dataPartitionPaths;
    this->logger = logger;
    this->pageSize = pageSize;
    this->usingDirect = false;
    this->cleared = false;
    // Initialize meta data;
    this->metaData = make_shared<PartitionedFileMetaData>();
    this->metaData->setPageSize(pageSize);
    this->metaData->setNumFlushedPages(0);
    this->metaData->setVersion(0);
    this->metaData->setLatestPageId((unsigned int)(-1));
    pthread_mutex_init(&this->fileMutex, nullptr);
    PartitionMetaDataPtr curPartitionMetaData;
    for (i = 0; i < dataPartitionPaths.size(); i++) {
        curPartitionMetaData = make_shared<PartitionMetaData>(dataPartitionPaths.at(i), i);
        this->metaData->addPartition(curPartitionMetaData);
    }

    // Initialize FILE instances;
    this->metaFile = nullptr;
    for (i = 0; i < dataPartitionPaths.size(); i++) {
        this->dataFiles.push_back(nullptr);
        this->dataHandles.push_back(-1);
    }
    this->openAll();
    this->writeMeta();

    // Initialize the mutex;
}

/**
 * Create a simple PartitionedFile instance, and later will add information
 * to the instance by parsing existing meta data or initialize the instance with nullptrs.
 */
PartitionedFile::PartitionedFile(NodeID nodeId,
                                 DatabaseID dbId,
                                 UserTypeID typeId,
                                 SetID setId,
                                 string metaPartitionPath,
                                 pdb::PDBLoggerPtr logger) {

    this->nodeId = nodeId;
    this->dbId = dbId;
    this->typeId = typeId;
    this->setId = setId;
    this->metaPartitionPath = metaPartitionPath;
    this->logger = logger;
    this->cleared = false;


    // Initialize the mutex;
    pthread_mutex_init(&this->fileMutex, nullptr);
}

/**
 * Destructor, it will NOT delete on-disk files.
 */
PartitionedFile::~PartitionedFile() {
    if (this->cleared == false) {
        pthread_mutex_lock(&this->fileMutex);
        this->closeAll();
        pthread_mutex_unlock(&this->fileMutex);
    }
    pthread_mutex_destroy(&this->fileMutex);
}

/**
 * Open meta partition only
 */
bool PartitionedFile::openMeta() {
    // check whether meta partition exists, if not, create it.
    if (this->metaFile != nullptr) {
        return false;
    }
    FILE* curFile;
    ifstream file(this->metaPartitionPath.c_str());
    if (!file) {
        curFile = fopen(this->metaPartitionPath.c_str(), "w");
    } else {
        curFile = fopen(this->metaPartitionPath.c_str(), "r+");
    }
    if (curFile != nullptr) {
        this->metaFile = curFile;
    } else {
        cout << "meta can't be open:" << this->metaPartitionPath.c_str() << "\n";
        return false;
    }
    return true;
}

/**
 * Open data partitions
 */
bool PartitionedFile::openData() {
    if (usingDirect == true) {
        return openDataDirect();
    }
    int numPartitions = this->dataPartitionPaths.size();
    int i;
    FILE* curFile;
    for (i = 0; i < numPartitions; i++) {
        curFile = fopen(this->dataPartitionPaths.at(i).c_str(), "a+");
        if (curFile != nullptr) {
            this->dataFiles.at(i) = curFile;
            cout << "file opened:" << this->dataPartitionPaths.at(i).c_str() << "\n";
        } else {
            cout << "file can't be open:" << this->dataPartitionPaths.at(i).c_str() << "\n";
            return false;
        }
    }
    return true;
}

/**
 * Open data partitions using direct I/O
 */
bool PartitionedFile::openDataDirect() {
    int numPartitions = this->dataPartitionPaths.size();
    int i;
    int handle;
    for (i = 0; i < numPartitions; i++) {
        handle = open(this->dataPartitionPaths.at(i).c_str(),
                      O_RDWR | O_APPEND | O_CREAT
#ifndef __APPLE__
                          |
                          O_DIRECT
#endif
                      ,
                      S_IRWXU | S_IRWXO);
        if (handle >= 0) {
            this->dataHandles.at(i) = handle;
            cout << "file opened:" << this->dataPartitionPaths.at(i).c_str() << "\n";
        } else {
            cout << "file can't be open:" << this->dataPartitionPaths.at(i).c_str() << "\n";
            return false;
        }
    }
    return true;
}

/**
 * Open meta partition and all data partitions
 */
bool PartitionedFile::openAll() {
    return ((this->openMeta()) && (this->openData()));
}


/**
 * Close meta partition and all data partitions
 */
bool PartitionedFile::closeAll() {
    if (usingDirect == true) {
        return closeDirect();
    }
    int i;
    int numPartitions = this->dataPartitionPaths.size();
    fclose(this->metaFile);
    this->metaFile = nullptr;
    for (i = 0; i < numPartitions; i++) {
        fclose(this->dataFiles.at(i));
        this->dataFiles.at(i) = nullptr;
    }
    return true;
}

/**
 * Close meta partition and all data partitions using direct I/O
 */
bool PartitionedFile::closeDirect() {
    int i;
    int numPartitions = this->dataPartitionPaths.size();
    fclose(this->metaFile);
    for (i = 0; i < numPartitions; i++) {
        close(this->dataHandles.at(i));
    }
    return true;
}


/**
 * To delete a file instance.
 */
void PartitionedFile::clear() {

    pthread_mutex_lock(&this->fileMutex);
    if (this->cleared == true) {

        pthread_mutex_unlock(&this->fileMutex);
        return;
    }
    this->closeAll();
    remove(this->metaPartitionPath.c_str());
    logger->info("PartitionedFile: Deleting file:" + this->metaPartitionPath);
    int i;
    int numPartitions = this->dataPartitionPaths.size();
    for (i = 0; i < numPartitions; i++) {
        remove(this->dataPartitionPaths.at(i).c_str());
        logger->info("PartitionedFile: Deleting file:" + this->dataPartitionPaths.at(i));
    }
    this->cleared = true;
    pthread_mutex_unlock(&this->fileMutex);
}

/**
 * Return a smart pointer pointing to the metaData of this PartitionedFile instance.
 */
PartitionedFileMetaDataPtr PartitionedFile::getMetaData() {
    return this->metaData;
}
/**
 * Append page to the partition identified by partitionId
 */
int PartitionedFile::appendPage(FilePartitionID partitionId, PDBPagePtr page) {
    if (usingDirect == true) {
        return appendPageDirect(partitionId, page);
    }
    FILE* curPartition = nullptr;
    if (((curPartition = this->dataFiles.at(partitionId)) == nullptr) || (page == nullptr)) {
        return -1;
    }

    PageID pageId = page->getPageID();

    pthread_mutex_lock(&this->fileMutex);
    if (this->cleared == true) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    if (this->writeData(curPartition, page->getRawBytes(), page->getRawSize()) < 0) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    // update metadata;
    this->metaData->incNumFlushedPages();

    if ((pageId > this->metaData->getLatestPageId()) ||
        (this->metaData->getLatestPageId() == (unsigned int)(-1))) {
        this->metaData->setLatestPageId(pageId);
    }

    // update partition metadata
    int ret = (int)(this->metaData->getPartition(partitionId)->getNumPages());
    this->metaData->addPageIndex(pageId, partitionId, ret);
    this->metaData->getPartition(partitionId)->incNumPages();
    pthread_mutex_unlock(&this->fileMutex);
    return ret;
}

/**
 * Append page using direct I/O
 */
int PartitionedFile::appendPageDirect(FilePartitionID partitionId, PDBPagePtr page) {
    int handle = -1;
    if (((handle = this->dataHandles.at(partitionId)) < 0) || (page == nullptr)) {
        return -1;
    }

    PageID pageId = page->getPageID();
    pthread_mutex_lock(&this->fileMutex);
    if (this->cleared == true) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    if (this->writeDataDirect(handle, page->getRawBytes(), page->getRawSize()) < 0) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    this->metaData->incNumFlushedPages();

    if ((pageId > this->metaData->getLatestPageId()) ||
        (this->metaData->getLatestPageId() == (unsigned int)(-1))) {
        this->metaData->setLatestPageId(pageId);
    }
    int ret = (int)(this->metaData->getPartition(partitionId)->getNumPages());
    this->metaData->addPageIndex(pageId, partitionId, ret);
    this->metaData->getPartition(partitionId)->incNumPages();
    pthread_mutex_unlock(&this->fileMutex);
    return ret;
}


/**
 * Initialize the meta partition, with following format:
 * - Metadata Size
 * - FileType
 * - Version
 * - PageSize
 * - NumFlushedPages
 * - LastFlushedPageID (Added Mar21,2016)
 * - NumPartitions
 * - PartitionID for 1st partition
 * - NumFlushedPages in 1st partition
 * - Length of the path to 1st partition
 * - Path to 1st partition
 * - PartitionID for 2nd partition
 * - NumFlushedPages in 2nd partition
 * - Length of the path to 2nd partition
 * - Path to 2nd partition
 * - ...
 * - PageId for the 1st page
 * - PartitionId for the 1st page
 * - PageSeqIdInPartition for the 1st page
 * - ...
 */
int PartitionedFile::writeMeta() {
    pthread_mutex_lock(&this->fileMutex);
    if (this->metaFile == nullptr) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    if (this->cleared == true) {
        pthread_mutex_unlock(&this->fileMutex);
        return -1;
    }
    // compute meta size
    size_t metaSize = sizeof(FileType) + sizeof(unsigned short) + sizeof(size_t) +
        sizeof(unsigned int) + sizeof(unsigned int) + sizeof(unsigned int);
    unsigned int numPartitions = this->dataPartitionPaths.size();
    unsigned int i = 0;
    for (i = 0; i < numPartitions; i++) {
        metaSize += sizeof(FilePartitionID) + sizeof(unsigned int) + sizeof(size_t) +
            this->dataPartitionPaths.at(i).length() + 1;
    }
    unsigned int numPages = this->metaData->getNumFlushedPages();
    for (i = 0; i < numPages; i++) {
        metaSize += sizeof(PageID) + sizeof(FilePartitionID) + sizeof(unsigned int);
    }
    // write meta size to meta partition
    fseek(this->metaFile, 0, SEEK_SET);
    fwrite((size_t*)(&metaSize), sizeof(size_t), 1, this->metaFile);
    fflush(this->metaFile);
    // allocate buffer for meta data
    char* buffer = (char*)malloc(metaSize * sizeof(char));
    char* cur = buffer;

    // intialize FileType
    *((FileType*)cur) = FileType::PartitionedFileType;
    cur = cur + sizeof(FileType);
    // initialize Version
    *((unsigned short*)cur) = this->metaData->getVersion();
    cur = cur + sizeof(unsigned short);
    // initialize PageSize
    *((size_t*)cur) = this->metaData->getPageSize();
    cur = cur + sizeof(size_t);
    // initialize TotalPageNumber
    *((unsigned int*)cur) = this->metaData->getNumFlushedPages();
    cur = cur + sizeof(unsigned int);
    *((unsigned int*)cur) = this->metaData->getLatestPageId();
    cur = cur + sizeof(unsigned int);
    // initialize Partitions
    *((unsigned int*)cur) = numPartitions;
    cur = cur + sizeof(unsigned int);

    for (i = 0; i < numPartitions; i++) {
        *((FilePartitionID*)cur) = i;
        cur = cur + sizeof(FilePartitionID);
        *((unsigned int*)cur) = this->metaData->getPartition(i)->getNumPages();
        cur = cur + sizeof(unsigned int);
        *((size_t*)cur) = this->dataPartitionPaths.at(i).length() + 1;
        cur = cur + sizeof(size_t);
        memcpy(cur,
               this->dataPartitionPaths.at(i).c_str(),
               this->dataPartitionPaths.at(i).length() + 1);
        cur = cur + this->dataPartitionPaths.at(i).length() + 1;
    }

    for (auto iter = this->getMetaData()->getPageIndexes()->begin();
         iter != this->getMetaData()->getPageIndexes()->end();
         iter++) {
        PageID pageId = iter->first;
        PageIndex pageIndex = iter->second;
        *((PageID*)cur) = pageId;
        cur = cur + sizeof(PageID);
        *((FilePartitionID*)cur) = pageIndex.partitionId;
        cur = cur + sizeof(FilePartitionID);
        *((unsigned int*)cur) = pageIndex.pageSeqInPartition;
        cur = cur + sizeof(unsigned int);
    }

    // write meta data
    fseek(this->metaFile, sizeof(size_t), SEEK_SET);
    int ret = this->writeData(this->metaFile, (void*)buffer, metaSize);
    fflush(this->metaFile);
    free(buffer);
    pthread_mutex_unlock(&this->fileMutex);
    return ret;
}

/**
 * Below function is buggy, please use writeMeta() instead.
 *
 *
 * Update the meta partition based on counters.
 * This can be used after a batch of flushing.
 * The difference between writeMeta() and updateMeta() is that the latter function will only update
 * a few fields.
 */
int PartitionedFile::updateMeta() {
    if (this->seekNumFlushedPagesInMeta() < 0) {
        return -1;
    }
    unsigned int numFlushedPages = this->getMetaData()->getNumFlushedPages();
    if (this->writeData(this->metaFile, &numFlushedPages, sizeof(unsigned int)) < 0) {
        return -1;
    }
    unsigned int numPartitions = this->dataPartitionPaths.size();
    unsigned int i;
    for (i = 0; i < numPartitions; i++) {
        this->seekNumFlushedPagesInPartitionMeta(i);
        numFlushedPages = this->getMetaData()->getPartition(i)->getNumPages();
        if (this->writeData(this->metaFile, &(numFlushedPages), sizeof(unsigned int)) < 0) {
            return -1;
        }
    }
    fflush(this->metaFile);
    return 0;
}

/**
 * To load the page from a given sequence/ordering number of the page
 * in the partition that is specified by partitionId.
 * This function will be invoked by PageCache instance,
 * length is the size of shared memory allocated for this load.
 * The PageCache instance should make sure that length == pageSize.
 * Otherwise, if length > pageSize, some space will be wasted, and
 * if length < pageSize, some data on the page may not get loaded.
 *
 * Return loaded size;
 */
size_t PartitionedFile::loadPage(FilePartitionID partitionId,
                                 unsigned int pageSeqInPartition,
                                 char* pageInCache,
                                 size_t length) {
    if (usingDirect == true) {
        return loadPageDirect(partitionId, pageSeqInPartition, pageInCache, length);
    }
    FILE* curFile = this->dataFiles.at(partitionId);
    if (curFile == nullptr) {
        return 0;
    }
    if (pageSeqInPartition < this->getMetaData()->getPartition(partitionId)->getNumPages()) {
        seekPage(curFile, pageSeqInPartition);
        return fread(pageInCache, sizeof(char), length, curFile);
    } else {
        return (size_t)-1;
    }
}

/**
 * To load page using direct I/O.
 */
size_t PartitionedFile::loadPageDirect(FilePartitionID partitionId,
                                       unsigned int pageSeqInPartition,
                                       char* pageInCache,
                                       size_t length) {
    int handle = this->dataHandles.at(partitionId);
    size_t ret;
    if (handle < 0) {
        return (size_t)(-1);
    }
    if (pageSeqInPartition < this->getMetaData()->getPartition(partitionId)->getNumPages()) {
        seekPageDirect(handle, pageSeqInPartition);
        ret = read(handle, pageInCache, length);
    } else {
        return (size_t)(-1);
    }
    return ret;
}

/**
 * To load the pageId for a specified page.
 * Return the pageId, if page exists, otherwise, return (unsigned int)(-1)
 */
PageID PartitionedFile::loadPageId(FilePartitionID partitionId, unsigned int pageSeqInPartition) {
    PageID ret = this->getMetaData()->getPageId(partitionId, pageSeqInPartition);
    return ret;
}

/**
 * Similar with above method.
 * The difference is this method will not seek, it just load sequentially,
 * so the caller needs to make sure current position is correct.
 */
size_t PartitionedFile::loadPageFromCurPos(FilePartitionID partitionId,
                                           unsigned int pageSeqInPartition,
                                           char* pageInCache,
                                           size_t length) {
    FILE* curFile = this->dataFiles.at(partitionId);
    if (curFile == nullptr) {
        return 0;
    }
    if (pageSeqInPartition < this->getMetaData()->getPartition(partitionId)->getNumPages()) {
        return fread(pageInCache, sizeof(char), length, curFile);
    } else {
        return (size_t)(-1);
    }
}


/**
  * Similar with above method.
  * The difference is this method will not seek, it just load sequentially,
  * so the caller needs to make sure current position is correct.
  */
PageID PartitionedFile::loadPageIdFromCurPos(FilePartitionID partitionId,
                                             unsigned int pageSeqInPartition,
                                             char* pageInCache,
                                             size_t length) {
    FILE* curFile = this->dataFiles.at(partitionId);
    if (curFile == nullptr) {
        return 0;
    }
    if (pageSeqInPartition < this->getMetaData()->getPartition(partitionId)->getNumPages()) {
        PageID pageId;
        size_t size = fread(&pageId, sizeof(char), sizeof(PageID), curFile);
        if (size == 0) {
            std::cout << "PartitionedFile: Read failed" << std::endl;
            return (PageID)(-1);
        }
        return pageId;
    } else {
        return (PageID)(-1);
    }
}

/**
 * Used when initialize PartitionedFile instance from metaPartition file on disk.
 * Read from the meta partition about numFlushedPages, set the numFlushedPages variable.
 * (This is true for the time being!)
 */
unsigned int PartitionedFile::getAndSetNumFlushedPages() {
    if (this->metaFile == nullptr) {
        return -1;
    }
    if (this->seekNumFlushedPagesInMeta() == 0) {
        this->logger->writeLn("PartitionedFile: get numFlushedPages from meta partition:");
        unsigned int numFlushedPages;
        size_t size =
            fread((unsigned int*)(&numFlushedPages), sizeof(unsigned int), 1, this->metaFile);
        if (size == 0) {
            std::cout << "PartitionedFile: Read failed" << std::endl;
            return 0;
        }
        this->getMetaData()->setNumFlushedPages(numFlushedPages);
        this->logger->writeInt(this->getMetaData()->getNumFlushedPages());
        return this->getMetaData()->getNumFlushedPages();
    } else {
        return 0;
    }
}

/**
 * Return numFlushedPages
 */
unsigned int PartitionedFile::getNumFlushedPages() {
    return this->getMetaData()->getNumFlushedPages();
}

/**
 * Return lastFlushedPageID
 */
PageID PartitionedFile::getLastFlushedPageID() {
    return this->getMetaData()->getLatestPageId();
}

/**
 * Return latestPageID (Note, actually it's the largest page id)
 */
PageID PartitionedFile::getLatestPageID() {
    return this->getMetaData()->getLatestPageId();
}

/**
 * Return NodeID of this file
 */
NodeID PartitionedFile::getNodeId() {
    return this->nodeId;
}

/**
 * Return DatabaseID of this file
 */
DatabaseID PartitionedFile::getDbId() {
    return this->dbId;
}


/**
 * Return UserTypeID of this file
 */
UserTypeID PartitionedFile::getTypeId() {
    return this->typeId;
}

/**
 * Return SetID of this file
 */
SetID PartitionedFile::getSetId() {
    return this->setId;
}

/**
 * To return the file type of the file: SequenceFile or PartitionedFile
 */
FileType PartitionedFile::getFileType() {
    return FileType::PartitionedFileType;
}

/**
 * To return the number of data partitions in the file
 */
unsigned int PartitionedFile::getNumPartitions() {
    return this->dataPartitionPaths.size();
}

/**
 * Set up meta data by parsing meta partition
 */
void PartitionedFile::buildMetaDataFromMetaPartition(SharedMemPtr shm) {
    // parse the meta file
    /**
     *  Meta partition format:
     * - Metadata Size
     * - FileType
     * - Version
     * - PageSize
     * - NumFlushedPages
         * - LatestPageID (Add on Mar 21, 2016)
     * - NumPartitions
     * - PartitionID for 1st partition
     * - NumFlushedPages in 1st partition
     * - Length of the path to 1st partition
     * - Path to 1st partition
     * - PartitionID for 2nd partition
     * - NumFlushedPages in 2nd partition
     * - Length of the path to 2nd partition
     * - Path to 2nd partition
     * - ...
         * - pageId for the 1st page
         * - FilePartitionID for the 1st page
         * - PageSeqIdInPartition for the 1st page
         * - ...
     */
    // Open meta partition for reading
    if (this->openMeta() == false) {
        this->logger->error("Fatal Error: PartitionedFile: Error: can't open meta partition.");
        exit(-1);
    }
    // get meta partition size;
    fseek(this->metaFile, 0, SEEK_SET);
    size_t size;
    size_t sizeRead = fread((size_t*)(&(size)), sizeof(size_t), 1, this->metaFile);
    if (sizeRead == 0) {
        std::cout << "PartitionedFile: Read meta size failed" << std::endl;
        exit(-1);
    }
    // load meta partition to memory
    fseek(this->metaFile, sizeof(size_t), SEEK_SET);
    char* buf = (char*)malloc(size * sizeof(char));
    sizeRead = fread((void*)buf, sizeof(char), size, this->metaFile);
    if (sizeRead < size) {
        cout << "Metadata corrupted, please remove storage folders and try again...\n";
        this->logger->error(
            "Fatal Error: Metadata corrupted, please remove storage folders and try again...");
        exit(-1);
    }

    // create a meta data instance
    this->metaData = make_shared<PartitionedFileMetaData>();

    // parse file type
    char* cur = buf;
    cur = cur + sizeof(FileType);

    // parse and set version;
    unsigned short version = (unsigned short)(*(unsigned short*)cur);
    this->metaData->setVersion(version);
    cur = cur + sizeof(unsigned short);

    // parse and set pageSize;
    size_t pageSize = (size_t)(*(size_t*)cur);
    this->metaData->setPageSize(pageSize);
    this->pageSize = pageSize;
    std::cout << "Detected file with page size=" << pageSize << std::endl;
    cur = cur + sizeof(size_t);

    // parse and set numFlushed pages;
    unsigned int numFlushedPages = (unsigned int)(*(unsigned int*)cur);
    this->metaData->setNumFlushedPages(numFlushedPages);
    cur = cur + sizeof(unsigned int);

    // parse and set latestPageId;
    unsigned int latestPageId = (unsigned int)(*(unsigned int*)cur);
    this->metaData->setLatestPageId(latestPageId);
    cur = cur + sizeof(unsigned int);


    // parse numPartitions;
    unsigned int numPartitions = (unsigned int)(*(unsigned int*)cur);
    cur = cur + sizeof(unsigned int);
    PartitionMetaDataPtr curPartitionMeta;
    FilePartitionID partitionId;
    unsigned int numFlushedPagesInPartition;
    size_t pathLen;

    // parse and set partition meta data
    unsigned int i;
    for (i = 0; i < numPartitions; i++) {
        curPartitionMeta = make_shared<PartitionMetaData>();
        // parse and set partitionId
        partitionId = (FilePartitionID)(*(FilePartitionID*)cur);
        curPartitionMeta->setPartitionId(partitionId);
        cur = cur + sizeof(FilePartitionID);

        // parse and set numFlushedPages
        numFlushedPagesInPartition = (unsigned int)(*(unsigned int*)cur);
        curPartitionMeta->setNumPages(numFlushedPagesInPartition);
        cur = cur + sizeof(unsigned int);

        // parse len
        pathLen = (size_t)(*(size_t*)cur);
        cur = cur + sizeof(size_t);
        // parse string
        string partitionPath(cur);
        this->dataPartitionPaths.push_back(partitionPath);
        curPartitionMeta->setPath(partitionPath);
        this->metaData->addPartition(curPartitionMeta);
        cur = cur + pathLen;
    }

    PageID pageId;
    unsigned int pageSeqInPartition;
    // parse and set page index data
    for (i = 0; i < numFlushedPages; i++) {
        pageId = (PageID)(*(PageID*)cur);
        cur = cur + sizeof(PageID);
        partitionId = (FilePartitionID)(*(FilePartitionID*)cur);
        cur = cur + sizeof(FilePartitionID);
        pageSeqInPartition = (unsigned int)(*(unsigned int*)cur);
        cur = cur + sizeof(unsigned int);
        this->metaData->addPageIndex(pageId, partitionId, pageSeqInPartition);
    }

    free(buf);
}

/**
 * Return page size of this file
 */
size_t PartitionedFile::getPageSize() {
    if (pageSize == 0) {
        pageSize = getPageSizeInMeta();
    }
    return pageSize;
}


/**
 * Read meta partition to return the pageSize of this file
 */
size_t PartitionedFile::getPageSizeInMeta() {
    if (this->metaFile == nullptr) {
        return (size_t)(-1);
    }
    if (this->seekPageSizeInMeta() == 0) {
        size_t pageSize;
        this->logger->writeLn("PartitionedFile: get page size from meta partition:");
        size_t sizeRead = fread((size_t*)(&(pageSize)), sizeof(size_t), 1, this->metaFile);
        if (sizeRead == 0) {
            std::cout << "PartitionedFile: Read failed" << std::endl;
            return 0;
        }
        this->logger->writeInt(pageSize);
        return pageSize;
    } else {
        return 0;
    }
}

/**
 * initialize data files;
 */
void PartitionedFile::initializeDataFiles() {
    unsigned int i;
    for (i = 0; i < dataPartitionPaths.size(); i++) {
        this->dataFiles.push_back(nullptr);
        this->dataHandles.push_back(-1);
    }
}

/**
 * Set dataPartitionPaths;
 */
void PartitionedFile::setDataPartitionPaths(const vector<string>& dataPartitionPaths) {
    this->dataPartitionPaths = dataPartitionPaths;
}


/**
 * Write data specified to the current file position.
 */
int PartitionedFile::writeData(FILE* file, void* data, size_t length) {

    if ((file == nullptr) || (data == nullptr)) {
        cout << "PartitionedFile: Error: writeData with nullptr.\n";
        return -1;
    }
    size_t retSize = fwrite(data, sizeof(char), length, file);
    fflush(file);
    if (retSize != length) {
        return -1;
    } else {
        return 0;
    }
}

/**
 * Write data specified to the current file position using direct I/O.
 */
int PartitionedFile::writeDataDirect(int handle, void* data, size_t length) {
    if ((handle < 0) || (data == nullptr)) {
        cout << "PartitionedFile: Error: invalid handle or data is nullptr.\n";
        return -1;
    }
    size_t retSize = write(handle, data, length);
    if (retSize != length) {
        cout << "written bytes:" << retSize << "\n";
        return -1;
    } else {
        return 0;
    }
}


/**
 * Seek to the beginning of the page data for a page specified in the file.
 */
int PartitionedFile::seekPage(FILE* partition, unsigned int pageSeqInPartition) {
    if (partition == nullptr) {
        return -1;
    }
    return fseek(partition, (pageSeqInPartition) * (this->metaData->getPageSize()), SEEK_SET);
}

/**
 * Seek to the beginning of the page data of the page specified in the file.
 */
int PartitionedFile::seekPageDirect(int handle, unsigned int pageSeqInPartition) {
    if (handle < 0) {
        return -1;
    }
    return lseek(handle, (pageSeqInPartition) * (this->metaData->getPageSize()), SEEK_SET);
}


/**
 * Seek to the page size field in meta data.
 */
int PartitionedFile::seekPageSizeInMeta() {
    if (this->metaFile == nullptr) {
        return -1;
    }
    return fseek(
        this->metaFile, sizeof(size_t) + sizeof(FileType) + sizeof(unsigned short), SEEK_SET);
}

/**
 * Seek to numFlushedPages field in meta data.
 */
int PartitionedFile::seekNumFlushedPagesInMeta() {
    if (this->metaFile == nullptr) {
        return -1;
    }
    return fseek(this->metaFile,
                 sizeof(size_t) + sizeof(FileType) + sizeof(unsigned short) + sizeof(size_t),
                 SEEK_SET);
}

/**
 * Seek to the numFlushedPages field in partition meta data.
 */
int PartitionedFile::seekNumFlushedPagesInPartitionMeta(FilePartitionID partitionId) {
    if (this->metaFile == nullptr) {
        return -1;
    }
    unsigned int i;
    unsigned int metaSize = sizeof(FileType) + sizeof(unsigned short) + sizeof(size_t) +
        sizeof(unsigned int) + sizeof(unsigned int);
    for (i = 0; i < partitionId; i++) {
        metaSize += sizeof(FilePartitionID) + sizeof(unsigned int) + sizeof(size_t) +
            this->dataPartitionPaths.at(i).length() + 1;
    }
    metaSize += sizeof(FilePartitionID);
    return fseek(this->metaFile, sizeof(size_t) + metaSize, SEEK_SET);
}
