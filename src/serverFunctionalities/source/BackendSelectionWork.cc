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
    //proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
    //PageHandlePtr destPageHandle = make_shared<PageHandle>(proxy, destPage);
    PageHandlePtr destPageHandle;

    auto queryProc = myQuery->getProcessor ();
    //queryProc->initialize ();
    //queryProc->loadOutputPage (destPage->getBytes(), destPage->getSize());

    pdb :: Handle< pdb :: Vector<pdb :: Handle<pdb :: String>>> outVec;

    while (this->iter->hasNext()) {
        page = this->iter->next(); 
        //page still can be nullptr, so we MUST check nullptr here.
        if (page != nullptr) {

            if (destPage == nullptr) {
                // we have never set a destPage before, initializing
                proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                destPageHandle = make_shared<PageHandle>(proxy, destPage);
                queryProc->initialize ();
                queryProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
            }


            //load input page
            std :: cout << "processing page with pageId=" << page->getPageID() << std :: endl;
            queryProc->loadInputPage (page->getBytes ());
            while (queryProc->fillNextOutputPage ()) {
                //Load new output page as we fill the current output page.
                destPageHandle->unpin();
                proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
                logger->writeLn("BackendSelectionWork: proxy pinned a new page with pageId=");
                logger->writeInt(destPage->getPageID());
                destPageHandle = make_shared<PageHandle>(proxy, destPage);
                queryProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
            }
            

            //clean the page;
            if (proxy->unpinUserPage(nodeId, page->getDbID(),
                    page->getTypeID(), page->getSetID(), page) == false) {
                logger->writeLn("BackendSelectionWork: cannot unpin finished page.");
                destPageHandle->unpin();
                callerBuzzer->buzz(PDBAlarm::QueryError);
                return;
            }
            logger->writeLn("BackendSelectionWork: send out unpinPage for source page with pageID:");
            logger->writeInt(page->getPageID());
        }
    }

    if (destPage != nullptr) {
        // we have ran something at least
        queryProc->finalize ();
        while (queryProc->fillNextOutputPage ()) {
            destPageHandle->unpin();
            proxy->addUserPage(destDatabaseId, destTypeId, destSetId, destPage);
            logger->writeLn("BackendSelectionWork: proxy pinned a new page with pageId=");
            logger->writeInt(destPage->getPageID());
            destPageHandle = make_shared<PageHandle>(proxy, destPage);
            queryProc->loadOutputPage (destPage->getBytes(), destPage->getSize());
        }
        destPageHandle->unpin();
    }
    
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
    return;
}

#endif
