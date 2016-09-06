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
 * File:   PDBFlushProducerWork.h
 * Author: Jia
 *
 * Created on October 22, 2015, 9:03 PM
 */

#ifndef PDBFLUSHPRODUCERWORK_H
#define	PDBFLUSHPRODUCERWORK_H

#include "PDBWork.h"
#include "PangeaStorageServer.h"
#include <memory>
using namespace std;
class PDBFlushProducerWork;
typedef shared_ptr<PDBFlushProducerWork> PDBFlushProducerWorkPtr;

class PDBFlushProducerWork : public pdb :: PDBWork {
public:
    PDBFlushProducerWork(pdb :: PangeaStorageServer * server);
    ~PDBFlushProducerWork() {
    };
    void execute(PDBBuzzerPtr callerBuzzer) override;

private:
    pdb :: PangeaStorageServer* server;

};



#endif	/* PDBFLUSHPRODUCERWORK_H */

