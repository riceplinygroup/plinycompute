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

#ifndef TEST_34_H
#define TEST_34_H

#include "Join.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "StorageClient.h"
#include "ChrisSelection.h"
#include "StringSelection.h"
#include "SharedEmployee.h"

using namespace pdb;

int main() {

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 128};

    // register this query class
    string errMsg;
    PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("clientLog");
    StorageClient temp(8108, "localhost", myLogger, true);
    // to register type
    // temp.registerType ("libraries/libChrisSelection.so", errMsg);
    // temp.registerType ("libraries/libStringSelection.so", errMsg);

    // connect to the query client
    QueryClient myClient(8108, "localhost", myLogger, true);

    // make the query graph
    Handle<Set<SharedEmployee>> myInputSet =
        myClient.getSet<SharedEmployee>("chris_db", "chris_set");
    Handle<ChrisSelection> myFirstSelect = makeObject<ChrisSelection>();
    myFirstSelect->setInput(myInputSet);
    Handle<StringSelection> mySecondSelect = makeObject<StringSelection>();
    mySecondSelect->setInput(myFirstSelect);
    Handle<QueryOutput<String>> outputOne =
        makeObject<QueryOutput<String>>("chris_db", "output_set1", myFirstSelect);
    Handle<QueryOutput<String>> outputTwo =
        makeObject<QueryOutput<String>>("chris_db", "output_set2", mySecondSelect);

    if (!myClient.execute(errMsg, outputOne, outputTwo)) {
        std::cout << "Query failed.  Message was: " << errMsg << "\n";
        return 0;
    }
}

#endif
