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

#ifndef PDBEVICTWORK_H
#define PDBEVICTWORK_H

#include <memory>
using namespace std;
class PDBEvictWork;
typedef shared_ptr<PDBEvictWork> PDBEvictWorkPtr;

#include "PDBWork.h"
#include "PDBBuzzer.h"
#include "PageCache.h"

/**
 * This class implements some work to do for cache eviction.
 */
class PDBEvictWork : public pdb::PDBWork {
public:
    PDBEvictWork(PageCache* cache);
    ~PDBEvictWork();

    // do the actual work.
    void execute(PDBBuzzerPtr callerBuzzer) override;

private:
    PageCache* cache;
};
#endif /* PDBEVICTWORK_H */
