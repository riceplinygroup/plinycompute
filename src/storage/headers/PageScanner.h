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
 * File:   PageScanner.h
 * Author: Jia
 *
 * Created on November 17, 2015, 10:04 AM
 */

#ifndef PAGESCANNER_H
#define	PAGESCANNER_H

#include "PDBCommunicator.h"
#include "PDBLogger.h"
#include "PageCircularBufferIterator.h"
#include "SharedMem.h"
#include "DataTypes.h"
#include <string.h>
#include <pthread.h>
#include <memory>
using namespace std;

class PageScanner;
typedef shared_ptr<PageScanner> PageScannerPtr;

/**
 * This class implements the page scanning procedure at backend server.
 * User first needs to call getSetIterators() to obtain a set of iterators
 * given the specified set information and number of threads.
 * User then needs to call recvPagesLoop() to receive pages from frontend,
 * and add pages to the concurrent blocking buffer,
 * that is an instance of PageCircularBuffer class.
 * At the end of recvPagesLoop(), it will receive a special message from frontend,
 * telling the scanner that all messages have been received and the recvPagesLoop()
 * can return.
 */
class PageScanner {

public:
    PageScanner(pdb :: PDBCommunicatorPtr communicator, SharedMemPtr shm,
            pdb :: PDBLoggerPtr logger, int numThreads, int recvBufSize, NodeID nodeId);
    ~PageScanner();

    /**
     * Obtain a set of iterators given the specified set information and number of threads.
     * Each iterator work as a consumer, retrieving a page from the concurrent blocking buffer,
     * each time when next() is invoked.
     */
    vector<PageCircularBufferIteratorPtr> getSetIterators(NodeID nodeId, DatabaseID dbId,
            UserTypeID typeId, SetID setId);

    /**
     * To receive PagePinned objects from frontend.
     */
    bool acceptPagePinned(pdb :: PDBCommunicatorPtr myCommunicator, string & errMsg, bool &morePagesToLoad, NodeID &dataNodeId, DatabaseID &dataDbId, UserTypeID &dataTypeId,
            SetID &dataSetId, PageID &dataPageId, size_t &pageSize, size_t &offset);

    /**
     * To send PagePinnedAck objects to frontend to acknowledge the receipt of PagePinned objects.
     */
    bool sendPagePinnedAck(pdb :: PDBCommunicatorPtr myCommunicator, bool wasError, string info, string & errMsg);

    /**
     * Receive pages from frontend, and add them to the concurrent blocking buffer,
     * that is an instance of PageCircularBuffer class.
     * At the end of recvPagesLoop(), it will receive a special message from frontend,
     * telling the scanner that all messages have been received and the recvPagesLoop()
     * can return.
     *
     * You can start multiple loops by assigning multiple different PDBCommunicators respectively.
     */
    bool recvPagesLoop(pdb :: PDBCommunicatorPtr myCommunicator);

    /**
     * Close the buffer
     */
    void closeBuffer();

    /**
     * Open the buffer
     */
    void openBuffer();


private:
    pdb :: PDBCommunicatorPtr communicator;
    pdb :: PDBLoggerPtr logger;
    PageCircularBufferPtr buffer;
    unsigned int numThreads;
    SharedMemPtr shm;
    NodeID nodeId;
};


#endif	/* PAGESCANNER_H */

