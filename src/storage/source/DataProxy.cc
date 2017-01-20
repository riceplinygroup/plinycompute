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

#include "PDBDebug.h"
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
#include "Configuration.h"

#ifndef MAX_RETRIES
#define MAX_RETRIES 5
#endif

DataProxy::DataProxy(NodeID nodeId, pdb :: PDBCommunicatorPtr communicator, SharedMemPtr shm, pdb :: PDBLoggerPtr logger) {
    this->nodeId = nodeId;
    this->communicator = communicator;
    this->shm = shm;
    this->logger = logger;
    this->communicator->setLongConnection(true);
}

DataProxy::~DataProxy() {
}

bool DataProxy::addTempSet(string setName, SetID &setId, bool needMem, int numTries) {
    if (numTries == MAX_RETRIES) {
        return false;
    }
    if (numTries > 0) {
        logger->error(std::string("DataProxy: addTempSet with numTries=")+std::to_string(numTries));
    }
    string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy: connection is closed" << std :: endl;
        logger->error("DataProxy: connection is closed, to reconnect");
        if (communicator->reconnect(errMsg)) {
            std :: cout << errMsg << std :: endl;
            logger->error(std::string("DataProxy: reconnect failed with errMsg")+errMsg);
           return false;
        }
    }
    if (needMem == true) {
        //create an AddSet object
        {
            const pdb::UseTemporaryAllocationBlock myBlock{1024};
            pdb::Handle<pdb :: StorageAddTempSet> msg = pdb::makeObject<pdb :: StorageAddTempSet>(setName);
            //we don't know the SetID to be added, the frontend will assign one.
            //send the message out
            if (!this->communicator->sendObject<pdb :: StorageAddTempSet> (msg, errMsg))   { 
               //We reserve Database 0 and Type 0 as temp data
               cout << "Sending object failure: " << errMsg <<"\n";
	       return addTempSet(setName, setId, needMem, numTries+1);
               
           }
        }
        //std :: cout << "sent StorageAddTempSet object to server." << std :: endl;

        //receive the StorageAddSetResult message
        {
            size_t objectSize = this->communicator->getSizeOfNextObject();
            if (objectSize == 0) {
                cout << "Receiving ack failure" << std :: endl;
                return addTempSet(setName, setId, needMem, numTries+1);
            }
            const pdb :: UseTemporaryAllocationBlock myBlock{objectSize};
            bool success;
            pdb :: Handle <pdb :: StorageAddTempSetResult> ack = 
	        this->communicator->getNextObject<pdb :: StorageAddTempSetResult> (success, errMsg) ;

            if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return addTempSet(setName, setId, needMem, numTries+1);
            }
            if (success == true) {
	        setId = ack->getTempSetID();
                //std :: cout << "Received StorageAddTempSetResult object from the server" << std :: endl;
            }     
            return success;
        }
    } else {

         {
             pdb::Handle<pdb :: StorageAddTempSet> msg = pdb::makeObject<pdb :: StorageAddTempSet>(setName);
             //we don't know the SetID to be added, the frontend will assign one.
             //send the message out
             if (!this->communicator->sendObject<pdb :: StorageAddTempSet> (msg, errMsg))   {
                //We reserve Database 0 and Type 0 as temp data
                cout << "Sending object failure: " << errMsg <<"\n";
                return addTempSet(setName, setId, needMem, numTries+1);
             }
         }
         //std :: cout << "sent StorageAddTempSet object to server." << std :: endl;

         //receive the StorageAddSetResult message
         {
             bool success;
             pdb :: Handle <pdb :: StorageAddTempSetResult> ack =
                  this->communicator->getNextObject<pdb :: StorageAddTempSetResult> (success, errMsg) ;
             if (ack == nullptr) {
                 cout << "Receiving ack failure:" << errMsg << "\n";
                 return addTempSet(setName, setId, needMem, numTries+1);
             }
             if(success == true) {
                  setId = ack->getTempSetID();
                  //std :: cout << "Received StorageAddTempSetResult object from the server" << std :: endl;
             }
             return success;
         }

    }
}


bool DataProxy::removeTempSet(SetID setId, bool needMem, int numTries) {
    if (numTries == MAX_RETRIES) {
        return false;
    }
    if (numTries > 0) {
        logger->error(std::string("DataProxy: removeTempSet with numTries=")+std::to_string(numTries));
    }
    string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy: connection is closed" << std :: endl;
        logger->error("DataProxy: connection is closed, to reconnect");
        if (communicator->reconnect(errMsg)) {
           std :: cout << errMsg << std :: endl;
           logger->error(std::string("DataProxy: reconnect failed with errMsg")+errMsg);
           return false;
        }
    }
    //create a RemoveSet object

    if (needMem == true) {
         {
             const pdb :: UseTemporaryAllocationBlock myBlock{1024};
             pdb :: Handle<pdb :: StorageRemoveTempSet> msg = pdb::makeObject<pdb :: StorageRemoveTempSet>(setId);

             //send the message out
             if (!this->communicator->sendObject<pdb :: StorageRemoveTempSet> (msg, errMsg)) {
                  cout << "Sending object failure: " << errMsg <<"\n";
	          return removeTempSet(setId, needMem, numTries+1);
             }
         }

         //receive the SimpleRequestResult message
         {
             bool success;
             size_t objectSize = this->communicator->getSizeOfNextObject();
             if (objectSize == 0) {
                 std :: cout << "Receiving ack failure" << std :: endl;
                 return removeTempSet(setId, needMem, numTries+1);
             }
             const pdb :: UseTemporaryAllocationBlock myBlock{objectSize};
             pdb :: Handle <pdb :: SimpleRequestResult> ack =
	           this->communicator->getNextObject<pdb :: SimpleRequestResult> (success, errMsg);
             if (ack == nullptr) {
                 cout << "Receiving ack failure:" << errMsg << "\n";
                 return removeTempSet(setId, needMem, numTries+1);
             }

             return success&&(ack->getRes().first);
         }
    }
    else {
         {
             pdb :: Handle<pdb :: StorageRemoveTempSet> msg = pdb::makeObject<pdb :: StorageRemoveTempSet>(setId);

             //send the message out
             if (!this->communicator->sendObject<pdb :: StorageRemoveTempSet> (msg, errMsg)) {
                 cout << "Sending object failure: " << errMsg <<"\n";
                 return removeTempSet(setId, needMem, numTries+1);
             }
          }

         //receive the SimpleRequestResult message
         {
              bool success;
              pdb :: Handle <pdb :: SimpleRequestResult> ack =
                  this->communicator->getNextObject<pdb :: SimpleRequestResult> (success, errMsg);
              if (ack == nullptr) {
                  cout << "Receiving ack failure:" << errMsg << "\n";
                  return removeTempSet(setId, needMem, numTries+1);
              }
              return success&&(ack->getRes().first);
          }

    }
}

//page will be pinned at Storage Server
bool DataProxy::addTempPage(SetID setId, PDBPagePtr &page, bool needMem, int numTries) {
    return addUserPage(0,0,setId, page, needMem, numTries);
}

bool DataProxy::addUserPage(DatabaseID dbId, UserTypeID typeId, SetID setId, PDBPagePtr &page, bool needMem, int numTries) {
    if (numTries == MAX_RETRIES) {
        return false;
    }
    if (numTries > 0) {
        logger->error(std::string("DataProxy: addTempPage with numTries=")+std::to_string(numTries));
    }
    string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy: connection is closed" << std :: endl;
        logger->error("DataProxy: connection is closed, to reconnect");
        if (communicator->reconnect(errMsg)) {
           std :: cout << errMsg << std :: endl;
           logger->error(std::string("DataProxy: reconnect failed with errMsg")+errMsg);
           return false;
        }
    }
    if (needMem == true) {
        //create a PinPage object
        {
            const pdb :: UseTemporaryAllocationBlock myBlock{2048};
            pdb::Handle<pdb :: StoragePinPage> msg = pdb::makeObject<pdb :: StoragePinPage>();
            //We reserve Database 0 and Type 0 as temp data
            msg->setNodeID(this->nodeId);
            msg->setDatabaseID(dbId);
            msg->setUserTypeID(typeId);
            msg->setSetID(setId);
            msg->setWasNewPage(true);
            //send the message out
            if (!this->communicator->sendObject<pdb :: StoragePinPage>(msg, errMsg)) {
                cout << "DataProxy.AddUserPage(): Sending object failure: " << errMsg <<"\n";
                return addUserPage(dbId, typeId, setId, page, needMem, numTries+1);
            }
        }
        
        //receive the PagePinned object
       {
            size_t objectSize = this->communicator->getSizeOfNextObject();
            if (objectSize == 0) {
                std :: cout << "Receive ack failure" << std :: endl;
                return addUserPage(dbId, typeId, setId, page, needMem, numTries+1);
            }
            PDB_COUT << "DataProxy: to allocate memory block for PagePinned object" << std :: endl;
            const pdb :: UseTemporaryAllocationBlock myBlock{objectSize};
            PDB_COUT << "DataProxy: memory block allocated" << std :: endl;
            bool success;
            pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned>(success, errMsg);
            if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return addUserPage(dbId, typeId, setId, page, needMem, numTries+1);
            }
            char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
            page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
            //cout << "ack->SetID()=" << ack->getSetID() << "\n";
            //cout << "page->SetID()=" << page->getSetID() << "\n";
            //cout << "ack->PageID()=" << ack->getPageID() << "\n";
            //cout << "page->PageID()=" << page->getPageID() << "\n";
            page->setPinned(true);
            page->setDirty(true);
            return success;
       }
    }
    else {

        //create a PinPage object
        {
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
                return addUserPage(dbId, typeId, setId, page, needMem, numTries+1);
            }
        }

        //receive the PagePinned object
        {
            bool success;
            pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned>(success, errMsg);
            if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return addUserPage(dbId, typeId, setId, page, needMem, numTries+1);
            }
            char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
            page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
            //cout << "ack->SetID()=" << ack->getSetID() << "\n";
            //cout << "page->SetID()=" << page->getSetID() << "\n";
            //cout << "ack->PageID()=" << ack->getPageID() << "\n";
            //cout << "page->PageID()=" << page->getPageID() << "\n";
            page->setPinned(true);
            page->setDirty(true);
            return success;
        }

   }
}

    
bool DataProxy::pinTempPage(SetID setId, PageID pageId, PDBPagePtr &page, bool needMem, int numTries) {
    return pinUserPage(this->nodeId, 0, 0, setId, pageId, page, needMem, numTries);
}

bool DataProxy::pinUserPage(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
        PageID pageId, PDBPagePtr &page, bool needMem, int numTries) {
    if (numTries == MAX_RETRIES) {
        return false;
    }
    if (numTries > 0) {
        logger->error(std::string("DataProxy: pinUserPage with numTries=")+std::to_string(numTries));
    }
    std :: string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy: connection is closed" << std :: endl;
        logger->error("DataProxy: connection is closed, to reconnect");
        if (communicator->reconnect(errMsg)) {
           std :: cout << errMsg << std :: endl;
           logger->error(std::string("DataProxy: reconnect failed with errMsg")+errMsg);
           return false;
        }
    }

    if(nodeId != this->nodeId) {
        this->logger->writeLn("DataProxy: We do not support to load pages from "
                "remote node for the time being.");
        return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1);
    }

    if (needMem == true) {
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
                return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1);
             }
        }

        //receive the PagePinned object
        {
             size_t objectSize = this->communicator->getSizeOfNextObject();
             if (objectSize == 0) {
                std :: cout << "Receiveing ack failure" << std :: endl;
                return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1); 
             }
             const pdb :: UseTemporaryAllocationBlock myBlock{objectSize};
             bool success;
             pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned> (success, errMsg);
             if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1);
             }
             char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
             page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
            page->setPinned(true);
            page->setDirty(false);
            return success;
        }
    } else {
        //create a PinPage object
        {
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
                return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1);
             }
        }

        //receive the PagePinned object
        {
             bool success;
             pdb :: Handle <pdb :: StoragePagePinned> ack =
                this->communicator->getNextObject<pdb :: StoragePagePinned> (success, errMsg);
             if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return pinUserPage(nodeId, dbId, typeId, setId, pageId, page, needMem, numTries+1);
             }
             char * dataIn = (char *) this->shm->getPointer(ack->getSharedMemOffset());
             page = make_shared<PDBPage>(dataIn, ack->getNodeID(), ack->getDatabaseID(), ack->getUserTypeID(), ack->getSetID(),
            ack->getPageID(), ack->getPageSize(), ack->getSharedMemOffset());
            page->setPinned(true);
            page->setDirty(false);
            return success;
        }

    }
}


bool DataProxy::unpinTempPage(SetID setId, PDBPagePtr page, bool needMem, int numTries) {
    return unpinUserPage(this->nodeId, 0,0,setId,page, needMem, numTries);
}

bool DataProxy::unpinUserPage(NodeID nodeId, DatabaseID dbId, UserTypeID typeId, SetID setId,
        PDBPagePtr page, bool needMem, int numTries) {
    if (numTries == MAX_RETRIES) {
        return false;
    }
    if (numTries > 0) {
        logger->error(std::string("DataProxy: unpinUserPage with numTries=")+std::to_string(numTries));
    }
    //std :: cout << "To unpin page with nodeId =" << nodeId << ", dbId=" << dbId << ", typeId=" << typeId << ", setId=" << setId << std :: endl;
    std :: string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy: connection is closed" << std :: endl;
        logger->error("DataProxy: connection is closed, to reconnect");
        if (communicator->reconnect(errMsg)) {
           std :: cout << errMsg << std :: endl;
           logger->error(std::string("DataProxy: reconnect failed with errMsg")+errMsg);
           return false;
        }
    }
    logger->debug(std :: string("Frontend to unpin page with dbId=")+std :: to_string(dbId)+std :: string(", typeId=")+std :: to_string(typeId) + std :: string(", setId=") + std :: to_string(setId) + std :: string(", pageId=") + std :: to_string(page->getPageID()));

    if (needMem == true) {
        //std :: cout << "we are going to use temporary allocation block to allocate unpin object" << std :: endl;
        //create a UnpinPage object
        {
           pdb :: UseTemporaryAllocationBlock myBlock{2048};

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
       
          //std :: cout << "To send StorageUnpinPage object with NodeID ="<< nodeId << ", DatabaseID=" <<
          //dbId << ", UserTypeID=" << typeId << ", SetID=" << setId << ", PageID=" << page->getPageID() << std :: endl;
          logger->debug("to send StorageUnpinPage object");
          //send the message out
          if (!this->communicator->sendObject<pdb :: StorageUnpinPage> (msg, errMsg)) {
              std :: cout << "Sending StorageUnpinPage object failure: " << errMsg <<"\n";
              logger->error(std :: string("Sending StorageUnpinPage object failure:")+errMsg);
	      return unpinUserPage(nodeId, dbId, typeId, setId, page, needMem, numTries+1);
          }
          //std :: cout << "StorageUnpinPage sent.\n";  
          logger->debug("StorageUnpinPage sent.");
       }

       //receive the Ack object
       {
           //std :: cout << "DataProxy received Unpin Ack with size=" << this->communicator->getSizeOfNextObject () << std :: endl;   
           size_t objectSize = this->communicator->getSizeOfNextObject();
           if (objectSize == 0) {
               std::cout << "receive ack failure" << std::endl;
               return unpinUserPage(nodeId, dbId, typeId, setId, page, needMem, numTries+1);
           }   
           pdb :: UseTemporaryAllocationBlock myBlock{objectSize};
           bool success;
           pdb :: Handle <pdb :: SimpleRequestResult> ack =
		this->communicator->getNextObject<pdb :: SimpleRequestResult>(success, errMsg);
           if(ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
                return unpinUserPage(nodeId, dbId, typeId, setId, page, needMem, numTries+1);
           }
           //std :: cout << "SimpleRequestResult for Unpin received." << std :: endl;
           return success&&(ack->getRes().first);
       }
    } else {
        //create a UnpinPage object
        {
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

          //std :: cout << "To send StorageUnpinPage object with NodeID ="<< nodeId << ", DatabaseID=" <<
          //dbId << ", UserTypeID=" << typeId << ", SetID=" << setId << ", PageID=" << page->getPageID() << std :: endl;

          //send the message out
          if (!this->communicator->sendObject<pdb :: StorageUnpinPage> (msg, errMsg)) {
              std :: cout << "Sending StorageUnpinPage object failure: " << errMsg <<"\n";
              return unpinUserPage(nodeId, dbId, typeId, setId, page, needMem, numTries+1);
          }
          //std :: cout << "StorageUnpinPage sent.\n";
       }

       //receive the Ack object
       {
           //std :: cout << "DataProxy received Unpin Ack with size=" << this->communicator->getSizeOfNextObject () << std :: endl;
           bool success;
           pdb :: Handle <pdb :: SimpleRequestResult> ack =
                this->communicator->getNextObject<pdb :: SimpleRequestResult>(success, errMsg);
           if (ack == nullptr) {
                cout << "Receiving ack failure:" << errMsg << "\n";
               return unpinUserPage(nodeId, dbId, typeId, setId, page, needMem, numTries+1);
           }
           //std :: cout << "SimpleRequestResult for Unpin received." << std :: endl;
           return success&&(ack->getRes().first);
       }

    }
}

PageScannerPtr DataProxy::getScanner(int numThreads) {
    std :: string errMsg;
    if (this->communicator->isSocketClosed() == true) {
        std :: cout << "ERROR in DataProxy.getScanner: connection is closed" << std :: endl;
        if (communicator->reconnect(errMsg)) {
           std :: cout << errMsg << std :: endl;
           return nullptr;
        }
    }
    if(numThreads <= 0) {
        return nullptr;
    }
    int scannerBufferSize;
    if (this->shm->getShmSize()/(DEFAULT_PAGE_SIZE) > 16) {
         scannerBufferSize = 3;
    } else {
	 scannerBufferSize = 1; 
    }    
    PageScannerPtr scanner = make_shared<PageScanner>(this->communicator, this->shm,
        this->logger, numThreads, scannerBufferSize, this->nodeId);
    return scanner;
}



#endif
