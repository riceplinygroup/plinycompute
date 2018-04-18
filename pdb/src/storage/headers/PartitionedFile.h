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
#ifndef PARTITIONEDFILE_H
#define PARTITIONEDFILE_H

#include "PDBFile.h"
#include "PartitionedFileMetaData.h"
#include "SharedMem.h"
#include <string>
#include <vector>
#include <memory>
using namespace std;
class PartitionedFile;
typedef shared_ptr<PartitionedFile> PartitionedFilePtr;

/**
 * This class wraps a PartitionedFile class that implements the PDBFileInterface.
 * If using PartitionedFile, each set will be flushed to multiple file partitions
 * in configured data directories.
 * The metadata will be flushed to a partition in the meta directory.
 *
 * Meta partition format:
 * - MetaSize
 * - FileType
 * - Version
 * - PageSize
 * - NumFlushedPages
 * - LatestPageID
 * - NumPartitions
 * - PartitionID for 1st partition
 * - NumFlushedPages in 1st partition
 * - Length of the path to 1st partition
 * - Path to 1st partition
 * - PartitionID for 2nd partition
 * - NumFlushedPages in 2nd partition
 * - Length of the path to 2nd partition
 * - Path to 2nd partition
 * ...
 * - PartitionID for PageID 1
 * - PageSeqIDInPartition for PageID 1
 * - PartitionID for PageID 2
 * - PageSeqIDInPartition for PageID 2
 * - PartitionID for PageID 3
 * - PageSeqIDInPartition for PageID 3
 * ...
 *
 * Data partition format:
 * - 1st pageId
 * - 1st page in the partition
 * - 2nd pageId
 * - 2nd page in the partition
 * - ...
 */
class PartitionedFile : public PDBFileInterface {
public:
    /**
     * Create a new PartitionedFile instance.
     */
    PartitionedFile(NodeID nodeId,
                    DatabaseID dbId,
                    UserTypeID typeId,
                    SetID setId,
                    string metaPartitionPath,
                    vector<string> dataPartitionPaths,
                    pdb::PDBLoggerPtr logger,
                    size_t pageSize);

    /**
     * Initialize a partitionedFile instance from existing meta data.
     */
    PartitionedFile(NodeID nodeId,
                    DatabaseID dbId,
                    UserTypeID typeId,
                    SetID setId,
                    string metaPartitionPath,
                    pdb::PDBLoggerPtr logger);

    /**
     * Destructor, it will NOT delete on-disk files.
     */
    ~PartitionedFile();

    /**
     * Open meta partition and all data partitions
     */
    bool openAll() override;

    /**
     * Open meta partition only
     */
    bool openMeta();

    /**
     * Open data partitions
     */
    bool openData();

    /**
     * Open data partitions using direct I/O
     */
    bool openDataDirect();

    /**
     * Close meta partition and all data partitions
     */
    bool closeAll() override;

    /**
     * Close meta partition and all data partitions using direct I/O
     */
    bool closeDirect();

    /**
     * To delete a file instance.
     */
    void clear() override;

    /**
     * Append page to the partition identified by partitionId
     * Return the PageSeqInPartition, if success, return -1 on failure.
     */
    int appendPage(FilePartitionID partitionId, PDBPagePtr page) override;

    /**
     * Append page using direct I/O
     */
    int appendPageDirect(FilePartitionID partitionId, PDBPagePtr page);

    /**
     * Initialize the meta partition
     */
    int writeMeta() override;


    /**
     * Update the meta partition
     */
    int updateMeta() override;

    /**
     * To load the page from a given sequence/ordering number of the page
     * in the partition that is specified by partitionId.
     * This function will be invoked by PageCache instance,
     * length is the size of shared memory allocated for this load.
     * The PageCache instance should make sure that length == pageSize.
     * Otherwise, if length > pageSize, some space will be wasted, and
     * if length < pageSize, some data on the page may not get loaded.
     */
    size_t loadPage(FilePartitionID partitionId,
                    unsigned int pageSeqInPartition,
                    char* pageInCache,
                    size_t length) override;

    /**
     * To load page using direct I/O.
     */
    size_t loadPageDirect(FilePartitionID partitionId,
                          unsigned int pageSeqInPartition,
                          char* pageInCache,
                          size_t length);


    /**
     * Similar with above method.
     * The difference is this method will not seek, it just load sequentially,
     * so the caller needs to make sure current position is correct.
     */
    size_t loadPageFromCurPos(FilePartitionID partitionId,
                              unsigned int pageSeqInPartition,
                              char* pageInCache,
                              size_t length);

    /**
     * To load the pageId for a specified page.
     * Return the pageId, if page exists, otherwise, return (unsigned int)(-1)
     */
    PageID loadPageId(FilePartitionID partitionId, unsigned int pageSeqInPartition);

    /**
      * Similar with above method.
      * The difference is this method will not seek, it just load sequentially,
      * so the caller needs to make sure current position is correct.
      */
    PageID loadPageIdFromCurPos(FilePartitionID partitionId,
                                unsigned int pageSeqInPartition,
                                char* pageInCache,
                                size_t length);


    /**
     * Read from the meta partition about lastFlushedPageId, set the lastFlushedPageId variable.
     * Also set the numFlushedPages variable as lastFlushedPageId + 1. (This is true for the time
     * being!)
     */
    unsigned int getAndSetNumFlushedPages() override;

    /**
     * Return numFlushedPages
     */
    unsigned int getNumFlushedPages() override;

    /**
     * Return lastFlushedPageID
     */
    PageID getLastFlushedPageID() override;

    /**
     * Return latestPageID
     */
    PageID getLatestPageID() override;

    /**
     * Return NodeID of this file
     */
    NodeID getNodeId() override;

    /**
     * Return DatabaseID of this file
     */
    DatabaseID getDbId() override;

    /**
     * Return UserTypeID of this file
     */
    UserTypeID getTypeId() override;

    /**
     * Return SetID of this file
     */
    SetID getSetId() override;

    /**
     * Return page size of this file
     */
    size_t getPageSize() override;

    /**
     * Read meta partition to get and set pageSize of this file
     */
    size_t getPageSizeInMeta() override;

    /**
     * To return the file type of the file: SequenceFile or PartitionedFile
     */
    FileType getFileType() override;

    /**
     * Return a smart pointer pointing to the metaData of this PartitionedFile instance.
     */
    PartitionedFileMetaDataPtr getMetaData();

    /**
     * initialize data files;
     */
    void initializeDataFiles();

    /**
     * Set dataPartitionPaths;
     */
    void setDataPartitionPaths(const vector<string>& dataPartitionPaths);

    /**
     * Set up meta data by parsing meta partition
     */
    void buildMetaDataFromMetaPartition(SharedMemPtr shm);

    /**
     * To return the number of data partitions in the file
     */
    unsigned int getNumPartitions();

protected:
    /**
     * Write data specified to the current file position.
     */
    int writeData(FILE* file, void* data, size_t length);

    /**
     * Write data specified to the current file position using direct I/O.
     */
    int writeDataDirect(int handle, void* data, size_t length);

    /**
     * Seek to the beginning of the page data of a page specified in the file.
     */
    int seekPage(FILE* file, unsigned int pageSeqInPartition);

    /**
     * Seek to the beginning of the page data of the page specified in the file.
     */
    int seekPageDirect(int handle, unsigned int pageSeqInPartition);

    /**
     * Seek to the page size field in meta data.
     */
    int seekPageSizeInMeta();

    /**
     * Seek to the numFlushedPages field in meta data.
     */
    int seekNumFlushedPagesInMeta();

    /**
     * Seek to the numFlushedPages field in partition meta data.
     */
    int seekNumFlushedPagesInPartitionMeta(FilePartitionID partitionId);


private:
    /**
     * Lock to synchronize delete and append operations
     */
    pthread_mutex_t fileMutex;


    /**
     * Meta file
     */
    FILE* metaFile = nullptr;
    // int metaHandle;

    /**
     * Data files
     */
    vector<FILE*> dataFiles;
    vector<int> dataHandles;

    /**
     * Path to meta partition
     */
    string metaPartitionPath;

    /**
     * Paths to data partitions
     */
    vector<string> dataPartitionPaths;

    /**
     * Logger instance
     */
    pdb::PDBLoggerPtr logger = nullptr;

    /**
     * Meta data instance
     */
    PartitionedFileMetaDataPtr metaData = nullptr;

    /**
     * NodeID of this PartitionedFile instance
     */
    NodeID nodeId;

    /**
     * DatabaseID of this PartitionedFile instance
     */
    DatabaseID dbId;

    /**
     * UserTypeID of this PartitionedFile instance
     */
    UserTypeID typeId;

    /**
     * SetID of this PartitionedFile instance
     */
    SetID setId;

    /**
     * Configured page size.
     */
    size_t pageSize = 0;

    /**
     * Using direct I/O or not
     */
    bool usingDirect;

    /**
     * whether the file is cleared
     */
    bool cleared;
};


#endif /* PARTITIONEDFILE_H */
