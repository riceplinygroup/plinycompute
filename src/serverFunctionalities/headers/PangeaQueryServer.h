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

#ifndef PANGEA_QUERY_SERVER_H
#define PANGEA_QUERY_SERVER_H

#include "ServerFunctionality.h"
#include "PDBServer.h"
#include <vector>
#include "PDBVector.h"
#include "QueryBase.h"

namespace pdb {

class PangeaQueryServer : public ServerFunctionality {

public:
    // creates a query server... the param is the number of threads to use
    // to answer queries
    PangeaQueryServer(int numThreads);

    // from the ServerFunctionality interface... registers the PangeaQueryServer's
    // handlers
    void registerHandlers(PDBServer& forMe) override;

    // this recursively traverses a simple query graph, where each node can only have one input,
    // makes sure that each node has been computed... if setOutputName is equal to "", then
    // the parameter setPrefix is the string that we'll use
    // to create each set name, whichNode is the counter that we use to name each set, and
    // computeMe is the node that we are wirred about computing
    void computeQuery(std::string setOutputName,
                      std::string setPrefix,
                      int& whichNode,
                      Handle<QueryBase>& computeMe,
                      std::vector<std::string>& tempSetsCreated);

    // this actually computes a selection query
    void doSelection(std::string setOutputName, Handle<QueryBase>& computeMe);

    // destructor
    ~PangeaQueryServer();

private:
    // the number of threads to use
    int numThreadsToUse;

    // used to count up temporary file names: tempSet0, tempSet1, tempSet2, ...
    int tempSetName;
};
}

#endif
