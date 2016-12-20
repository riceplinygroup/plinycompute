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
 * PDBScanWork.h
 *
 *  Created on: Dec 25, 2015
 *      Author: Jia
 */

#ifndef SRC_CPP_MAIN_STORAGE_HEADERS_PDBSCANWORK_H_
#define SRC_CPP_MAIN_STORAGE_HEADERS_PDBSCANWORK_H_

#include "PDBBuzzer.h"
#include "PageCircularBufferIterator.h"
#include "PDBCommunicator.h"
#include "PangeaStorageServer.h"
#include <memory>
using namespace std;
class PDBScanWork;
typedef shared_ptr<PDBScanWork> PDBScanWorkPtr;

/**
 * This class illustrates how a FrontEnd server scan set data and communicate with BackEnd server
 * to notify loaded pages.
 */

class PDBScanWork : public pdb :: PDBWork {
public:

    PDBScanWork(PageIteratorPtr iter, pdb :: PangeaStorageServer * storage, int & counter);
    ~PDBScanWork();
    bool sendPagePinned(pdb :: PDBCommunicatorPtr myCommunicator, bool morePagesToPin, NodeID nodeId, DatabaseID dbId, UserTypeID typeId,
            SetID setId, PageID pageId, size_t pageSize, size_t offset);

    bool acceptPagePinnedAck(pdb :: PDBCommunicatorPtr myCommunicator, bool &wasError, string &info, string & errMsg);

    // do the actual work
    void execute(PDBBuzzerPtr callerBuzzer) override;

private:

    PageIteratorPtr iter;
    pdb :: PangeaStorageServer * storage;
    int & counter;
    pthread_mutex_t connection_mutex;

};





#endif /* SRC_CPP_MAIN_STORAGE_HEADERS_PDBSCANWORK_H_ */
