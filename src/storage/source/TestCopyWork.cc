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
#ifndef TESTCOPYWORK_CC
#define TESTCOPYWORK_CC

#include "TestCopyWork.h"
#include "PDBPage.h"
#include "Handle.h"
#include "Object.h"
#include "DataProxy.h"
#include "PageHandle.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBVector.h"
#include <string>
#include "SharedEmployeeCopy.h"

using namespace std;

TestCopyWork::TestCopyWork(PageCircularBufferIteratorPtr iter, DatabaseID destDatabaseId, UserTypeID destTypeId, SetID destSetId, pdb :: HermesExecutionServer * server, int & counter): counter(counter) {
    this->iter = iter;
    this->destDatabaseId = destDatabaseId;
    this->destTypeId = destTypeId;
    this->destSetId = destSetId;
    this->server = server;
}

// do the actual work

void TestCopyWork::execute(PDBBuzzerPtr callerBuzzer) {
    char logName[100];
    sprintf(logName, "thread%d.log", iter->getId());
    pdb :: PDBLoggerPtr logger = make_shared<pdb :: PDBLogger>(logName);
    logger->writeLn("TestCopyWork: running...");
    
    //create a new connection to frontend server
    string errMsg;
    pdb :: PDBCommunicatorPtr communicatorToFrontEnd = make_shared<pdb :: PDBCommunicator>();
    communicatorToFrontEnd->connectToInternetServer(logger, server->getConf()->getPort(), 
            "localhost", errMsg);

    NodeID nodeId = server->getNodeID();
    DataProxyPtr proxy = make_shared<DataProxy>(nodeId, communicatorToFrontEnd, this->server->getSharedMem(), logger);

    PDBPagePtr page, destPage;
    proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
    //std :: cout << "databaseId  = " << destDatabaseId << std :: endl;
    //std :: cout << "typeId = " << destTypeId << std :: endl;
    //std :: cout << "setId  = " << destSetId << std :: endl;
    PageHandlePtr destPageHandle = make_shared<PageHandle>(proxy, destPage);
    //std :: cout << "databaseId of destPage = " << destPage->getDbID() << std :: endl;
    //std :: cout << "typeId of destPage = " << destPage->getTypeID() << std :: endl;
    //std :: cout << "setId of destPage = " << destPage->getSetID() << std :: endl;

    //load output page
    pdb :: UseTemporaryAllocationBlockPtr blockPtr;
    blockPtr = std :: make_shared <pdb :: UseTemporaryAllocationBlock>(destPageHandle->getWritableBytes(), destPageHandle->getWritableSize());
    pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> outputVec =
                pdb :: makeObject<pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> (300000);
    pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> inputVec; 
    while (this->iter->hasNext()) {
        page = this->iter->next(); 
        //page still can be nullptr, so we MUST check nullptr here.
        if (page != nullptr) {

            //load input page
            std :: cout << "processing page with pageId=" << page->getPageID() << std :: endl;
            pdb :: Record < pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> * temp = (pdb :: Record < pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> *) page->getBytes();
            inputVec = temp->getRootObject();
            std :: cout << "there are "<< inputVec->size() << " objects on the page."<< std :: endl;
 

            for (int i = 0; i < inputVec->size(); i++) {
                pdb :: Handle<pdb :: SharedEmployeeCopy> object = (*inputVec)[i];
                if (i%10000 == 0) {
                    std :: cout << i << ":";
                    object->print();
                    std :: cout << std :: endl;
                }
                try {
                    outputVec->push_back(object);
                }
                catch (pdb :: NotEnoughSpace &n) {
                    std :: cout << "########################################\n";
                    std :: cout << "a page is fully written with "<<outputVec->size()<<" objects\n";
                    getRecord(outputVec);
                    //std :: cout << "TestCopyWork:"<<iter->getId()<<", unpin a destination page with pageId=" << destPageHandle->getPageID()<<"\n";
                    destPageHandle->unpin();
                    //std :: cout << "TestCopyWork:"<< iter->getId()<< "unpinned a destination page with pageId=" << destPageHandle->getPageID()<<"\n";
                    proxy->addUserPage(this->destDatabaseId, this->destTypeId, this->destSetId, destPage);
                    logger->writeLn("TestCopyWork: proxy pinned a new temp page with pageId=");
                    logger->writeInt(destPage->getPageID());
                    //std :: cout << "TestCopyWork:"<<iter->getId()<< ": proxy pinned a new destination page with pageId=" <<destPage->getPageID()<<"\n";
                    destPageHandle = make_shared<PageHandle>(proxy, destPage);
           
                    blockPtr = nullptr;
                    blockPtr = std :: make_shared <pdb :: UseTemporaryAllocationBlock>(destPageHandle->getWritableBytes(), destPageHandle->getWritableSize());
                    outputVec = pdb :: makeObject<pdb :: Vector<pdb :: Handle<pdb :: SharedEmployeeCopy>>> (300000);
                    outputVec->push_back(object);

                }
            }
            //std :: cout <<iter->getId()<< ": copied " << inputVec->size() << " objects for source page with pageID: " << page->getPageID() << ".\n";
            //clean the page;
            if (proxy->unpinUserPage(nodeId, page->getDbID(),
                    page->getTypeID(), page->getSetID(), page) == false) {
                logger->writeLn("TestCopyWork: can not unpin finished page.");
                destPageHandle->unpin();
                callerBuzzer->buzz(PDBAlarm::QueryError);
                return;
            }
            logger->writeLn("TestCopyWork: send out unpinPage for source page with pageID:");
            logger->writeInt(page->getPageID());
            //std :: cout << iter->getId()<<": unpinned source page with pageID:" << page->getPageID() << ".\n";
        }
        //std :: cout << iter->getId() << ": to get next page" << std :: endl;
    }
    std :: cout << "########################################\n";
    std :: cout << "a page is fully written with "<<outputVec->size()<<" objects\n";
    getRecord(outputVec);
    //std :: cout << "TestCopyWork: to unpin the last destination page with pageId=" << destPageHandle->getPageID() << "\n"; 
    destPageHandle->unpin();
    //std :: cout << "TestCopyWork:"<< iter->getId()<< "unpinned a destination page with pageId=" << destPageHandle->getPageID()<<"\n";
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
    return;
}

#endif
