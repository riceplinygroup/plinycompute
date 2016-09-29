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
#ifndef BACKENDSELECTIONWORK_CC
#define BACKENDSELECTIONWORK_CC

#include "BackendSelectionWork.h"
#include "PDBPage.h"
#include "Handle.h"
#include "Object.h"
#include "DataProxy.h"
#include "PageHandle.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBVector.h"
#include <string>
#include "SharedEmployee.h"

using namespace std;

void BackendSelectionWork::execute(PDBBuzzerPtr callerBuzzer) {

    size_t tempPageSize = 1024 * 1 * 1024;

    char logName[100];
    sprintf(logName, "thread%d.log", iter->getId());
    pdb :: PDBLoggerPtr logger = make_shared<pdb :: PDBLogger>(logName);
    logger->writeLn("BackendSelectionWork: running...");

    //create a new connection to frontend server
    string errMsg;
    pdb :: PDBCommunicatorPtr communicatorToFrontEnd = make_shared<pdb :: PDBCommunicator>();
    communicatorToFrontEnd->connectToInternetServer(logger, server->getConf()->getPort(), 
            "localhost", errMsg);

    NodeID nodeId = server->getNodeID();
    DataProxyPtr proxy = make_shared<DataProxy>(nodeId, communicatorToFrontEnd, this->server->getSharedMem(), logger);

    PDBPagePtr page, destPage;
    PageHandlePtr destPageHandle;
    void *tempPage = 0;

    auto filterProc = myQuery->getFilterProcessor();
    auto projProc = myQuery->getProjectionProcessor();
    while (this->iter->hasNext()) {
        page = this->iter->next(); 
        //page still can be nullptr, so we MUST check nullptr here.
        if (page != nullptr) {
            //tempPageSize = page->getSize();
            if (tempPage == 0) {
                // we never did any query work before in this thread.
                tempPage = calloc(tempPageSize, 1);
                filterProc->initialize();
                filterProc->loadOutputPage(tempPage, tempPageSize);

            }

            // load input page
            std :: cout << "processing page with pageId=" << page->getPageID() << std :: endl;
            filterProc->loadInputPage (page->getBytes ());
            while (filterProc->fillNextOutputPage()) {

                // now tempPage is the input page for projection
                // the tempPage was filled, use it as input for projection
                projProc->loadInputPage(tempPage);
                if (destPage == nullptr) {
                    proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                    destPageHandle = make_shared<PageHandle>(proxy, destPage); 
                    projProc->initialize ();
                    projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());   
                }
                
                
                while (projProc->fillNextOutputPage()) {
                    destPageHandle->unpin();
                    proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                    logger->writeLn("BackendSelectionWork: proxy pinned a new page with pageId=");
                    logger->writeInt(destPage->getPageID());
                    destPageHandle = make_shared<PageHandle>(proxy, destPage);
                    projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
                }
                
                
                projProc->finalize();
                projProc->fillNextOutputPage();
                projProc->clearOutputPage();
                destPageHandle->unpin();
                destPage = nullptr;    

                std::cout << "Done part of projection " << std::endl;
                // finished processing tempPage, load a new one.
                filterProc->clearOutputPage();
                free(tempPage);
                tempPage = calloc(tempPageSize, 1);
                
                std::cout << "loading new temppage " << std::endl;
                filterProc->loadOutputPage(tempPage, tempPageSize);
                std::cout << "done loading new temppage " << std::endl;

            }

            //clean the page;
            if (proxy->unpinUserPage(nodeId, page->getDbID(),
                    page->getTypeID(), page->getSetID(), page) == false) {
                logger->writeLn("BackendSelectionWork: cannot unpin finished page.");
                destPageHandle->unpin();
                filterProc->clearOutputPage();
                free(tempPage);
                callerBuzzer->buzz(PDBAlarm::QueryError);
                return;
            }
            logger->writeLn("BackendSelectionWork: send out unpinPage for source page with pageID:");
            logger->writeInt(page->getPageID());
        }
    }

    if (tempPage != 0) {
        // we have actually ran something at the very least

        filterProc->finalize();
        while (filterProc->fillNextOutputPage()) {
            // the tempPage was filled, use it as input for projection
            if (destPage == nullptr) {
                proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                destPageHandle = make_shared<PageHandle>(proxy, destPage);
                projProc->initialize ();
                projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
            }
            projProc->loadInputPage(tempPage);
            while (projProc->fillNextOutputPage()) {
                destPageHandle->unpin();
                proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                logger->writeLn("BackendSelectionWork: proxy pinned a new page with pageId=");
                logger->writeInt(destPage->getPageID());
                destPageHandle = make_shared<PageHandle>(proxy, destPage);
                projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
            }

            projProc->finalize();
            projProc->fillNextOutputPage();
            projProc->clearOutputPage();
            destPageHandle->unpin();
            destPage = nullptr;

            filterProc->clearOutputPage();
            // finished processing tempPage, load a new one.
            free(tempPage);
            tempPage = calloc(tempPageSize, 1);
            filterProc->loadOutputPage(tempPage, tempPageSize);
        }

        // finally, finish the projection on leftover records.
        if (destPage == nullptr) {
            proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
            destPageHandle = make_shared<PageHandle>(proxy, destPage);
            projProc->initialize ();
            projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
        }
        projProc->loadInputPage(tempPage);
        while (projProc->fillNextOutputPage ()) {
            destPageHandle->unpin();
            proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
            logger->writeLn("BackendSelectionWork: proxy pinned a new page with pageId=");
            logger->writeInt(destPage->getPageID());
            destPageHandle = make_shared<PageHandle>(proxy, destPage);
            projProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
        }
        projProc->finalize();
        projProc->fillNextOutputPage ();
        projProc->clearOutputPage();
        //destPageHandle->unpin();
        filterProc->clearOutputPage();
        //free(tempPage);
    }
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
    return;
}

#endif
