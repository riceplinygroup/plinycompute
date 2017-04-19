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
 * File:   SequenceFile.cc
 * Author: Jia
 *
 */


#include "SequenceFile.h"
#include <stdio.h>
#include <string.h>
#include <string>
#include <iostream>

using namespace std;


//if isNew == true, the path is the root path, we need to new a sequence file
//otherwise, we just open existing sequence file

SequenceFile::SequenceFile(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId, string path, pdb :: PDBLoggerPtr logger, size_t pageSize) {
    this->nodeId = nodeId;
    this->dbId = dbId;
    this->typeId = typeId;
    this->setId = setId;
    this->file = nullptr;
    this->metaSize = sizeof (FileType) + sizeof (size_t);
    this->filePath = path;
    this->logger = logger;
    this->numFlushedPages = 0;
    this->pageSize = pageSize;
    logger->writeLn("SequenceFile: path:");
    logger->writeLn(filePath.c_str());
    this->openAll();
}

SequenceFile::~SequenceFile() {
    if (this->file != nullptr) {
        this->closeAll();
    }
}

bool SequenceFile::openAll() {
    if (this->file != nullptr) {
        return true;
    }
    if ((this->file = fopen(this->filePath.c_str(), "a+")) != 0) {
        return true;
    } else {
        this->logger->writeLn("SequenceFile: file can not be opened");
        perror(nullptr);
        return false;
    }
}

bool SequenceFile::closeAll() {
    if (fclose(this->file) == 0) {
        this->file = nullptr;
        return true;
    } else {
        return false;
    }
}

void SequenceFile::clear() {
    this->closeAll();
    if(this->file != nullptr) {
        this->file = nullptr;
        this->numFlushedPages = 0;
    }
    //delete the file
    if( remove(this->filePath.c_str()) == 0) {
        cout<< "Removed temp data " << this->filePath <<".\n";
        this->filePath = "";
    } 
}

int SequenceFile::appendPage(FilePartitionID partitionId, PDBPagePtr page) {
	return appendPage(page);
}

int SequenceFile::appendPage(PDBPagePtr page) {
    if(this->file == nullptr){
        return -1;
    }
    this->logger->writeLn("SequenceFile: appending data...");
    int retSize = fwrite(page->getRawBytes(), sizeof (char), page->getSize(), this->file);
    PageID pageId = page->getPageID();
    this->logger->writeLn("SequenceFile: PageID:");
    this->logger->writeInt(pageId);
    retSize += fwrite(&pageId, sizeof(PageID), 1, this->file);
    fflush(this->file);
    this->numFlushedPages++;
    this->lastFlushedId = pageId;
    this->logger->writeLn("SequenceFile: appendPage: Size:");
    this->logger->writeInt(retSize);
    this->logger->writeLn("SequenceFile: appendPage: PageID:");
    this->logger->writeInt((this->lastFlushedId));
    return 0;
}

int SequenceFile::writeData(void * data, size_t length) {
    if(this->file == nullptr){
        return -1;
    }
    size_t retSize = fwrite(data, sizeof (char), length, this->file);
    if (retSize != length) {
        return -1;
    } else {
        return 0;
    }
}



//Meta data is always at the beginning of a file, and has following layout:
//FileType: sizeof(enum)
//PageSize: sizeof(size_t)
int SequenceFile::writeMeta() {
    if(this->file == nullptr){
        return -1;
    }
    char* buffer = new char[this->metaSize];
    char * cur = buffer;
    //initialize FileType
    * ((FileType *) cur) = FileType::SequenceFileType;
    cur = cur + sizeof (FileType);
    //initialize PageSize;
    * ((size_t *) cur) = this->pageSize;
    int ret = this->writeData(buffer, this->metaSize);
    delete[] buffer;
    return ret;
}

int SequenceFile::updateMeta () {
	return 0;
}

int SequenceFile::seekPageSizeInMeta() {
    if(this->file == nullptr){
        return -1;
    }
    return fseek(this->file, sizeof (FileType), SEEK_SET);
}

size_t SequenceFile::getPageSizeInMeta() {
    if(this->file == nullptr){
        return -1;
    }
    if (this->seekPageSizeInMeta() == 0) {
        size_t pageSize;
        this->logger->writeLn("SequenceFile: get page size from file meta:");
        size_t sizeRead=fread((size_t *) (&(pageSize)), sizeof (size_t), 1, this->file);
        if (sizeRead == 0) {
            return 0;
        }
        this->logger->writeInt(pageSize);
        return pageSize;
    } else {
        return 0;
    }
}

int SequenceFile::seekLastFlushedPageID() {
    if(this->file == nullptr){
        return -1;
    }
    return fseek(this->file, -sizeof (PageID), SEEK_END);
}

unsigned int SequenceFile::getAndSetNumFlushedPages() {
    if (this->seekLastFlushedPageID() == 0) {
        size_t size = ftell(this->file);
        this->logger->writeLn("SequenceFile: file position after seek:");
        this->logger->writeInt(size);
        if (size <= this->metaSize) {
            this->logger->writeLn("SequenceFile: no flushedPages.");
            this->numFlushedPages = 0;
        }
        this->logger->writeLn("SequenceFile: set numFlushedPages:");
        size_t sizeRead = fread((PageID *) (&(this->lastFlushedId)), sizeof (PageID), 1, this->file);
        if (sizeRead == 0) {
            std :: cout << "SequenceFile: Read failed" << std :: endl;
            return 0;
        }
        this->numFlushedPages = this->lastFlushedId + 1;
        this->logger->writeInt(this->lastFlushedId);
    } else {
        this->logger->writeLn("SequenceFile: no flushedPages.");
        this->numFlushedPages = 0;
    }
    return this->numFlushedPages;
}

unsigned int SequenceFile::getNumFlushedPages() {
    return this->numFlushedPages;
}

PageID SequenceFile::getLastFlushedPageID() {
    return this->lastFlushedId;
}

PageID SequenceFile::getLatestPageID() {
    return this->lastFlushedId;
}

int SequenceFile::seekPage(PageID pageId) {
    if(this->file == nullptr){
        return -1;
    }
    return fseek(this->file, this->metaSize + (pageId)*(this->pageSize + sizeof (PageID)), SEEK_SET);
}

//Load data of given length on the page specified to cache
size_t SequenceFile::loadPage(PageID pageId, char * pageInCache, size_t length) {
	return loadPage(0, pageId, pageInCache, length);
}

//Load data of given length on the page specified to cache
size_t SequenceFile::loadPage(FilePartitionID partitionId, unsigned int pageSeqInPartition, char * pageInCache, size_t length) {
    if(this->file == nullptr){
        return 0;
    }
    seekPage(pageSeqInPartition);
    return fread(pageInCache, sizeof (char), length, this->file);
}


int SequenceFile::seekEnd() {
    if(this->file == nullptr){
        return -1;
    }
    return fseek(this->file, 0, SEEK_END);
}


