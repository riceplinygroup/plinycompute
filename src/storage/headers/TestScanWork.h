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
 * File:   TestScanWork.h
 * Author: Jia (modified from Chris' BackEndQueryOneRunWork class.)
 *
 * Created on November 16, 2015, 10:22 AM
 */

#ifndef TESTSCANWORK_H
#define TESTSCANWORK_H

#include "PDBBuzzer.h"
#include "PageCircularBufferIterator.h"
#include "HermesExecutionServer.h"
#include <memory>
using namespace std;
class TestScanWork;
typedef shared_ptr<TestScanWork> TestScanWorkPtr;

/**
 * This class illustrates how a backend server can communicate with frontend server
 * to scan user data stored as a set in storage.
 */

class TestScanWork : public pdb::PDBWork {
public:
    TestScanWork(PageCircularBufferIteratorPtr iter,
                 pdb::HermesExecutionServer* server,
                 int& counter);

    // do the actual work
    void execute(PDBBuzzerPtr callerBuzzer) override;

private:
    PageCircularBufferIteratorPtr iter;
    pdb::HermesExecutionServer* server;
    int& counter;
};


#endif /* TESTSCANWORK_H */
