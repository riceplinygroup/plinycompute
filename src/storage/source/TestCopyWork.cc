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
using namespace std;

TestCopyWork::TestCopyWork(PageCircularBufferIteratorPtr iter, DatabaseID destDatabaseId, UserTypeID destTypeId, SetID destSetId, pdb :: HermesExecutionServer * server) {
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
    PageHandlePtr destPageHandle = make_shared<PageHandle>(proxy, destPage);
 
    while (this->iter->hasNext()) {
        page = this->iter->next(); 
        //page still can be nullptr, so we MUST check nullptr here.
        if (page != nullptr) {

            //load input page
            std :: cout << "processing page with pageId=" << page->getPageID() << std :: endl;
            pdb :: Record < pdb :: Vector<pdb :: Handle<pdb :: Object>>> * temp = (pdb :: Record < pdb :: Vector<pdb :: Handle<pdb :: Object>>> *) page->getBytes();
            pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: Object>>> objects = temp->getRootObject();
            std :: cout << "there are "<< objects->size() << " objects on the page."<< std :: endl;
 
            //load output page
            const pdb :: UseTemporaryAllocationBlock myBlock{destPageHandle->getWritableBytes(), destPageHandle->getWritableSize()};
            pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: Object>>> outputObjects = 
                pdb :: makeObject<pdb :: Vector<pdb :: Handle<pdb :: Object>>> (10);
            pdb :: Vector<pdb :: Handle<pdb :: Object>> & outVec = *(outputObjects);

            for (int i = 0; i < objects->size(); i++) {
                pdb :: Handle<pdb :: Object> object = (*objects)[i];
                try {
                    outVec.push_back(object);
                }
                catch (pdb :: NotEnoughSpace &n) {
                    std :: cout << "TestCopyWork:"<<iter->getId()<<", unpin a destination page with pageId=" << destPageHandle->getPageID()<<"\n";
                    destPageHandle->unpin();
                    proxy->addUserPage(this->destDatabaseId, this->destTypeId, this->destSetId, destPage);
                    logger->writeLn("TestCopyWork: proxy pinned a new temp page with pageId=");
                    logger->writeInt(destPage->getPageID());
                    std :: cout << "TestCopyWork:"<<iter->getId()<< ", proxy pinned a new destination page with pageId=" <<destPage->getPageID()<<"\n";
                    destPageHandle = make_shared<PageHandle>(proxy, destPage);
                    const pdb :: UseTemporaryAllocationBlock myBlock{destPageHandle->getWritableBytes(), destPageHandle->getWritableSize()};
                    pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: Object>>> outputObjects = 
                               pdb :: makeObject<pdb :: Vector<pdb :: Handle<pdb :: Object>>> (10);
                    outVec = *(outputObjects);
                }
            }
            std :: cout <<iter->getId()<< ": copied " << objects->size() << " objects for source page with pageID: " << page->getPageID() << ".\n";
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
            std :: cout << iter->getId()<<", send out unpinPage for source page with pageID:" << page->getPageID() << ".\n";
        }
    }
    destPageHandle->unpin();
    std :: cout << "TestCopyWork:"<< iter->getId()<< "unpinned a destination page with pageId=" << destPageHandle->getPageID()<<"\n";
    callerBuzzer->buzz(PDBAlarm::WorkAllDone);
    return;
}

#endif
