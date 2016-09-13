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
 * File:   DataProxy.cc
 * Author: Jia
 *
 */


#ifndef DATA_PROXY_H
#define DATA_PROXY_H

#include "DataProxy.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Object.h"
#include "Handle.h"
#include "StorageAddTempSet.h"
#include "StorageAddTempSetResult.h"
#include "StoragePinPage.h"
#include "StorageUnpinPage.h"
#include "SimpleRequestResult.h"
#include "StoragePagePinned.h"
#include "StorageRemoveTempSet.h"
#include "CloseConnection.h"

#define DATA_PROXY_SCANNER_BUFFER_SIZE 3

DataProxy::DataProxy(NodeID nodeId, pdb :: PDBCommunicatorPtr communicator, SharedMemPtr shm, pdb :: PDBLoggerPtr logger) {
    this->nodeId = nodeId;
    this->communicator = communicator;
    this->shm = shm;
    this->logger = logger;
}

DataProxy::~DataProxy() {
}

bool DataProxy::addTempSet(string setName, SetID &setId) {
    string errMsg;
    //create an AddSet object
    {
        const pdb::UseTemporaryAllocationBlock myBlock{1024};
        pdb::Handle<pdb :: StorageAddTempSet> msg = pdb::makeObject<pdb :: StorageAddTempSet>(setName);
        //we don't know the SetID to be added, the frontend will assign one.
        //send the message out
        if (!this->communicator->sendObject<pdb :: StorageAddTempSet> (msg, errMsg))   { 
            //We reserve Database 0 and Type 0 as temp data
            cout << "Sending object failure: " << errMsg <<"\n";
	    return false;
        }
    }
    //receive the StorageAddSetResult message
    {
        const pdb :: UseTemporaryAllocationBlock myBlock{this->communicator->getSizeOfNextObject()};
        bool success;
        pdb :: Handle <pdb :: StorageAddTempSetResult> ack = 
	    this->communicator->getNextObject<pdb :: StorageAddTempSetResult> (success, errMsg) ;
        if(success == true) {
	    setId = ack->getTempSetID();
        }     
        return success;
    }
    
}


bool DataProxy::removeTempSet(SetID setId) {
    string errMsg;
    
    //create a RemoveSet object
    {
         const pdb :: UseTemporaryAllocationBlock myBlock{1024};
         pdb :: Handle<pdb :: StorageRemoveTempSet> msg = pdb::makeObject<pdb :: StorageRemoveTempSet>();

         //We reserve Database 0 and Type 0 as temp data
         msg->setDatabaseID(0);
         msg->setUserTypeID(0);
         msg->setSetID(setId);
         //send the message out
         if (!this->communicator->sendObject<pdb :: StorageRemoveTempSet> (msg, errMsg)) {
              cout << "Sending object failure: " << errMsg <<"\n";
	      return false;
         }
    }

    //receive the SimpleRequestResult message
    {
         bool success;
         const pdb :: UseTemporaryAllocationBlock myBlock{this->communicator->getSizeOfNextObject()};
         pdb :: Handle <pdb :: SimpleRequestResult> ack =
	     this->communicator->getNextObject<pdb :: SimpleRequestResult> (success, errMsg);
         return success&&(ack->getRes().first);
    }
}

//page will be pinned at Storage Server
bool DataProxy::addTempPage(SetID setId, PDBPagePtr &page) {
    return addUserPage(0,0,setId, page);
}

bool DataProxy::addUserPage(DatabaseID dbId, UserTypeID typeId, SetID setId, PDBPagePtr &page) {
    string errMsg;
    //create a PinPage object
    {
        const pdb :: UseTemporaryAllocationBlock myBlock{1024};
        pdb::Handle<pdb :: StoragePinPage> msg = pdb::makeObject<pdb :: StoragePinPage>();
        //We reserve Database 0 and Type 0 as temp data
        msg->setNodeID(this->nodeId);
        msg->setDatabaseID(dbId);
        msg->setUserTypeID(typeId);
        msg->setSetID(setId);
        msg->setWasNewPage(true);
        //send the message out
        if (!this->communicator->sendObject<pdb :: StoragePinPage>(msg, errMsg)) {
                cout << "Sending object failure: " << errMsg <<"\n";
                return false;
        }
    }
        
    //receive the PagePinned object
    {
        const pdb :: UseTemporaryAllocationBlock myBlock{this->communicator->getSizeOfNextObject()};
        bool success;
        pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned>(success, errMsg);
        char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
        page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
        //cout << "ack->getPageID()=" << ack->getPageID() << "\n";
        //cout << "page->getPageID()=" << page->getPageID() << "\n";
        page->setPinned(true);
        page->setDirty(true);
        return success;
    }
}

    
bool DataProxy::pinTempPage(SetID setId, PageID pageId, PDBPagePtr &page) {
    return pinUserPage(this->nodeId, 0, 0, setId, pageId, page);
}

bool DataProxy::pinUserPage(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
        PageID pageId, PDBPagePtr &page) {

    if(nodeId != this->nodeId) {
        this->logger->writeLn("DataProxy: We do not support to load pages from "
                "remote node for the time being.");
        return false;
    }

    string errMsg;
    //create a PinPage object
    {
         const pdb :: UseTemporaryAllocationBlock myBlock{1024};
         pdb::Handle<pdb :: StoragePinPage> msg = pdb::makeObject<pdb :: StoragePinPage>();
         msg->setNodeID(nodeId);
         msg->setDatabaseID(dbId);
         msg->setUserTypeID(typeId);
         msg->setSetID(setId);
         msg->setPageID(pageId);
         msg->setWasNewPage(false);

         //send the message out
        if (!this->communicator->sendObject<pdb :: StoragePinPage> (msg, errMsg)) {
                cout << "Sending object failure: " << errMsg <<"\n";
                return false;
        }
    }

    //receive the PagePinned object
    {
        const pdb :: UseTemporaryAllocationBlock myBlock{this->communicator->getSizeOfNextObject ()};
        bool success;
        pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned> (success, errMsg);
        char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
        page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
        page->setPinned(true);
        page->setDirty(false);
        return success;
    }
}


bool DataProxy::unpinTempPage(SetID setId, PDBPagePtr page) {
    return unpinUserPage(this->nodeId, 0,0,setId,page);
}

bool DataProxy::unpinUserPage(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
        PDBPagePtr page) {

    string errMsg;
    //create a UnpinPage object
    {
        pdb :: UseTemporaryAllocationBlock myBlock{1024};
        pdb::Handle<pdb :: StorageUnpinPage> msg = pdb::makeObject<pdb :: StorageUnpinPage>();

        msg->setNodeID(nodeId);
        msg->setDatabaseID(dbId);
        msg->setUserTypeID(typeId);
        msg->setSetID(setId);
        msg->setPageID(page->getPageID());

       if(page->isDirty() == true) {
          msg->setWasDirty(true);
       } else {
          msg->setWasDirty(false);
       }

       //send the message out
       if (!this->communicator->sendObject<pdb :: StorageUnpinPage> (msg, errMsg)) {
          cout << "Sending object failure: " << errMsg <<"\n";
	  return false;
       }
       //cout << "message sent.\n";  
    }

    //receive the Ack object
    {      
        pdb :: UseTemporaryAllocationBlock myBlock{this->communicator->getSizeOfNextObject ()};
        bool success;
        pdb :: Handle <pdb :: SimpleRequestResult> ack =
		this->communicator->getNextObject<pdb :: SimpleRequestResult>(success, errMsg);
        return success&&(ack->getRes().first);
    }
}

PageScannerPtr DataProxy::getScanner(int numThreads) {
    if(numThreads <= 0) {
        return nullptr;
    }
    PageScannerPtr scanner = make_shared<PageScanner>(this->communicator, this->shm,
        this->logger, numThreads, DATA_PROXY_SCANNER_BUFFER_SIZE, this->nodeId);
    return scanner;
}



#endif
