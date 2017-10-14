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
 * DistributedStorageCleanup.h
 *
 *  Created on: Nov 16, 2016
 *      Author: Jia
 */

#ifndef SRC_BUILTINPDBOBJECTS_HEADERS_DISTRIBUTEDCLEANUP_H_
#define SRC_BUILTINPDBOBJECTS_HEADERS_DISTRIBUTEDCLEANUP_H_


#include "Object.h"

//  PRELOAD %DistributedStorageCleanup%

namespace pdb {
// this object type is sent to the server to tell it there is no more page to load, scan finished at
// frontend
class DistributedStorageCleanup : public pdb::Object {

public:
    DistributedStorageCleanup() {}
    ~DistributedStorageCleanup() {}

    ENABLE_DEEP_COPY
};
}

#endif /* SRC_BUILTINPDBOBJECTS_HEADERS_DISTRIBUTEDCLEANUP_H_ */
