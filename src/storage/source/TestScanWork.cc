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
#ifndef TESTSCANWORK_CC
#define TESTSCANWORK_CC

#include "TestScanWork.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "PDBPage.h"
#include "DataProxy.h"
#include <string>
using namespace std;

TestScanWork::TestScanWork(PageCircularBufferIteratorPtr iter,
                           pdb::HermesExecutionServer* server,
                           int& counter)
    : counter(counter) {
    this->iter = iter;
    this->server = server;
}

// do the actual work

void TestScanWork::execute(PDBBuzzerPtr callerBuzzer) {
    pdb::PDBLoggerPtr logger = server->getLogger();
    logger->writeLn("TestScanWork: running...");

    // create a new connection to frontend server
    string errMsg;
    pdb::PDBCommunicatorPtr communicatorToFrontEnd = make_shared<pdb::PDBCommunicator>();
    communicatorToFrontEnd->connectToInternetServer(
        logger, server->getConf()->getPort(), "localhost", errMsg);

    NodeID nodeId = server->getNodeID();
    DataProxyPtr proxy = make_shared<DataProxy>(
        nodeId, communicatorToFrontEnd, this->server->getSharedMem(), logger);

    PDBPagePtr page;
    while (this->iter->hasNext()) {
        page = this->iter->next();
        // page still can be nullptr, so we MUST check nullptr here.
        if (page != nullptr) {
            std::cout << "processing page with pageId=" << page->getPageID() << std::endl;
            pdb::Record<pdb::Vector<pdb::Handle<pdb::Object>>>* temp =
                (pdb::Record<pdb::Vector<pdb::Handle<pdb::Object>>>*)page->getBytes();
            pdb::Handle<pdb::Vector<pdb::Handle<pdb::Object>>> objects = temp->getRootObject();
            std::cout << "there are " << objects->size() << " objects on the page." << std::endl;

            // clean the page;
            if (proxy->unpinUserPage(
                    nodeId, page->getDbID(), page->getTypeID(), page->getSetID(), page) == false) {
                logger->writeLn("TestScanWork: can not add finished page to cleaner.");
                callerBuzzer->buzz(PDBAlarm::QueryError);
                return;
            }
            // std :: cout << "send out unpinPage for page with pageID:" << page->getPageID() << "."
            // << std :: endl;
        }
    }
    callerBuzzer->buzz(PDBAlarm::WorkAllDone, counter);
    return;
}

#endif
