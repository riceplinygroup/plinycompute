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

// Added by Leo to test pipeline stuff and to avoid linker issues

#ifndef PIPELINE_DUMMY_TEST_SERVER_H
#define PIPELINE_DUMMY_TEST_SERVER_H

#include "ServerFunctionality.h"
#include "PDBServer.h"
#include <vector>
#include "PDBVector.h"
#include "QueryBase.h"

namespace pdb {

class PipelineDummyTestServer : public ServerFunctionality {

public:
    PipelineDummyTestServer();

    void registerHandlers(PDBServer& forMe) override;

    ~PipelineDummyTestServer();
};
}

#endif
