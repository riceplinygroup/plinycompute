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
 * PDBFlushConsumerWork.h
 *
 *  Created on: Dec 27, 2015
 *      Author: Jia
 */

#ifndef SRC_CPP_MAIN_STORAGE_HEADERS_PDBFLUSHCONSUMERWORK_H_
#define SRC_CPP_MAIN_STORAGE_HEADERS_PDBFLUSHCONSUMERWORK_H_

#include "PDBWork.h"
#include "PangeaStorageServer.h"
#include <memory>
using namespace std;
class PDBFlushConsumerWork;
typedef shared_ptr<PDBFlushConsumerWork> PDBFlushConsumerWorkPtr;

class PDBFlushConsumerWork : public pdb::PDBWork {
public:
    PDBFlushConsumerWork(FilePartitionID partitionId, pdb::PangeaStorageServer* server);
    ~PDBFlushConsumerWork(){};
    void execute(PDBBuzzerPtr callerBuzzer) override;
    void stop();

private:
    pdb::PangeaStorageServer* server;
    FilePartitionID partitionId;
    bool isStopped;
};


#endif /* SRC_CPP_MAIN_STORAGE_HEADERS_PDBFLUSHCONSUMERWORK_H_ */
