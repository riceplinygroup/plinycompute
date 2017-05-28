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

#ifndef STORAGE_COLLECT_STATS_RESPONSE_H
#define STORAGE_COLLECT_STATS_RESPONSE_H

#include "Object.h"
#include "Handle.h"
#include "PDBString.h"
#include "PDBVector.h"
#include "SetIdentifier.h"

// PRELOAD %StorageCollectStatsResponse%

namespace pdb {

// encapsulates a request to return all user set information
class StorageCollectStatsResponse  : public Object {

public:

	StorageCollectStatsResponse () {}
	~StorageCollectStatsResponse () {}

        Handle<Vector<Handle<SetIdentifier>>> & getStats() {
            return stats;
        }


	ENABLE_DEEP_COPY

private:

        Handle<Vector<Handle<SetIdentifier>>> stats;


};

}

#endif
