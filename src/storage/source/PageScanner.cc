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
 * File:   PageScanner.cc
 * Author: Jia
 *
 */


#ifndef PAGESCANNER_CC
#define PAGESCANNER_CC

#include "PageScanner.h"
#include "PDBCommunicator.h"
#include "PageCircularBufferIterator.h"
#include "PDBPage.h"
#include "SharedMem.h"
#include "StorageGetSetPages.h"
#include "StoragePagePinned.h"
#include "SimpleRequestResult.h"
#include "UseTemporaryAllocationBlock.h"
#include "Handle.h"
#include "InterfaceFunctions.h"
#include <vector>
#include <iostream>
using namespace std;

PageScanner::PageScanner(pdb :: PDBCommunicatorPtr communicator, SharedMemPtr shm,
        pdb :: PDBLoggerPtr logger,
        int numThreads, int recvBufSize, NodeID nodeId) {
    this->communicator = communicator;
    this->shm = shm;
    this->logger = logger;
    this->numThreads = numThreads;
    this->buffer = make_shared<PageCircularBuffer>(recvBufSize, logger);
    this->nodeId = nodeId;
}

PageScanner::~PageScanner() {
}

/**
 * To receive PagePinned objects from frontend.
 */
bool PageScanner::acceptPagePinned(pdb :: PDBCommunicatorPtr myCommunicator, string & errMsg, bool & morePagesToLoad, NodeID & dataNodeId, DatabaseID & dataDbId, UserTypeID & dataTypeId,
        SetID & dataSetId, PageID & dataPageId, size_t & pageSize,  size_t & offset) {

    if (myCommunicator == nullptr) {
        return false;
    }

    //receive the PinPage object
    size_t receivedSize = myCommunicator->getSizeOfNextObject();
    if (receivedSize == 0) {
         std :: cout << "ERROR in PageScanner: received size is 0" << std :: endl;
         return false;
    }
    const pdb :: UseTemporaryAllocationBlock myBlock{receivedSize};
    bool success;
    pdb :: Handle <pdb :: StoragePagePinned> msg =
	myCommunicator->getNextObject<pdb :: StoragePagePinned> (success, errMsg);
    if(success == true) {
	morePagesToLoad = msg->getMorePagesToLoad();
	dataNodeId = msg->getNodeID();
	dataDbId = msg->getDatabaseID();
	dataTypeId = msg->getUserTypeID();
	dataSetId = msg->getSetID();
	dataPageId = msg->getPageID();
	pageSize = msg->getPageSize();
	offset = msg->getSharedMemOffset();
    }
    return success;
}

/**
 * To send PagePinnedAck objects to frontend to acknowledge the receipt of PagePinned objects.
 */
bool PageScanner::sendPagePinnedAck(pdb :: PDBCommunicatorPtr myCommunicator, bool wasError, string info, string & errMsg) {
        const pdb :: UseTemporaryAllocationBlock myBlock{1024};
        pdb::Handle<pdb :: SimpleRequestResult> ack = pdb::makeObject<pdb :: SimpleRequestResult>(!wasError, errMsg);
	if (!myCommunicator->sendObject<pdb :: SimpleRequestResult> (ack, errMsg)) {
		cout << "Sending object failure: " << errMsg <<"\n";
		return false;
	}
        return true;
}


vector<PageCircularBufferIteratorPtr> PageScanner::getSetIterators(NodeID nodeId,
        DatabaseID dbId, UserTypeID typeId, SetID setId) {
        //std :: cout << "BackEndServer: send GetSetPagesMsg to frontEnd.\n";
	//create an GetSetPages object
	string errMsg;
        const pdb :: UseTemporaryAllocationBlock myBlock{1024};
        pdb::Handle<pdb :: StorageGetSetPages> getSetPagesRequest = pdb::makeObject<pdb :: StorageGetSetPages>();
	getSetPagesRequest->setDatabaseID(dbId);
	getSetPagesRequest->setUserTypeID(typeId);
	getSetPagesRequest->setSetID(setId);

        vector<PageCircularBufferIteratorPtr> vec;
        //send request to storage
        if (!this->communicator->sendObject<pdb :: StorageGetSetPages> (getSetPagesRequest, errMsg)) {
            errMsg = "Could not send data to server.";
            logger->error(std::string("PageScanner: ") + errMsg);
            return vec;
        }

        //initialize iterators;
        unsigned int i;
        PageCircularBufferIteratorPtr iter;
        for (i = 0; i < this->numThreads; i++) {
            iter = make_shared<PageCircularBufferIterator> (i, this->buffer, this->logger);
            vec.push_back(iter);
        }
        return vec;
}

bool PageScanner::recvPagesLoop(pdb :: Handle<pdb :: StoragePagePinned> pinnedPage, pdb :: PDBCommunicatorPtr myCommunicator) {
    PDB_COUT << "PageScanner: recvPagesLoop processing.\n";

    string errMsg;
    bool morePagesToLoad = pinnedPage->getMorePagesToLoad();
    NodeID dataNodeId = pinnedPage->getNodeID();
    DatabaseID dataDbId = pinnedPage->getDatabaseID();
    UserTypeID dataTypeId = pinnedPage->getUserTypeID();
    SetID dataSetId = pinnedPage->getSetID();
    PageID dataPageId = pinnedPage->getPageID();
    size_t pageSize = pinnedPage->getPageSize();
    size_t offset = pinnedPage->getSharedMemOffset();
    PDBPagePtr page;
    bool ret;

    //Due to the new handling mechanism, we need to process the first message then accept the next message;
    do  {
        PDB_COUT << "dataPageId:"<<dataPageId<<"\n";
        PDB_COUT << "morePagesToLoad:"<<morePagesToLoad<<"\n";
        logger->debug(string("got a page with pageId=")+to_string(dataPageId));
        //if there are no more pages to send at the frontend side, send ACK and return.
        if (morePagesToLoad == false) {
            PDB_COUT << "BackEndServer: sending Ack to frontEnd to end loop...\n";
            logger->debug(string("BackEndServer: sending Ack to frontEnd to end loop...\n"));
            this->sendPagePinnedAck(myCommunicator, false, "", errMsg);
            PDB_COUT << "BackEndServer: sent Ack to frontend to end loop...\n";
            logger->debug(string("BackEndServer: sent Ack to frontend to end loop...\n"));
            return true;
        }
        //if there are more pages to send at the frontend side,
        //we wrap the page object, add it to buffer, and send back ack.
        else {
            char * rawData = (char *) this->shm->getPointer(offset);
            char * refCountBytes = rawData + (sizeof(NodeID) + sizeof(DatabaseID) + sizeof(UserTypeID) + sizeof(SetID) + sizeof(PageID));
            int numObjects = *((int *) refCountBytes);
            std :: cout << "There are " << numObjects << " objects in the page" << std :: endl;
            page = make_shared<PDBPage>(rawData, dataNodeId, dataDbId,
                     dataTypeId, dataSetId, dataPageId, pageSize, offset, 0, numObjects);
            std :: cout << "BackEndServer: add page with " << page->getEmbeddedNumObjects()  << "to circular buffer...\n";
            logger->debug(string( "BackEndServer: add page scanner page to circular buffer...\n"));
            if (this->buffer != nullptr) {
                this->buffer->addPageToTail(page);
            } else {
                std :: cout << "Fatal Error: this is bad, the circular buffer is null!" << std :: endl;
                logger->error("Fatal Error: this is bad, the circular buffer is null!");
                return true;
            }
            PDB_COUT << "BackEndServer: sending PagePinnedAck to frontEnd...\n";
            logger->debug("BackEndServer: sending PagePinnedAck to frontEnd...\n");
            this->sendPagePinnedAck(myCommunicator, false, "", errMsg);
            PDB_COUT << "BackEndServer: sent PagePinnedAck to frontEnd...\n";
            logger->debug("BackEndServer: sent PagePinnedAck to frontEnd...\n");
            
        }
    }     while ((ret = this->acceptPagePinned(myCommunicator, errMsg, morePagesToLoad, dataNodeId, dataDbId, dataTypeId,
            dataSetId, dataPageId, pageSize, offset)) == true);
    PDB_COUT << "PageScanner Work is done" << endl;
    logger->debug("PageScanner Work is done");
    return false;
}

/**
 * Close the buffer
 */
void PageScanner::closeBuffer() {
	this->buffer->close();
}
/**
 * Open the buffer
 */
void PageScanner::openBuffer() {
       this->buffer->open();
}
#endif
