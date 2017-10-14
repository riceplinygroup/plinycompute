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

#ifndef TEST_48_H
#define TEST_48_H

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
#include "Set.h"
#include "Supervisor.h"
#include "QueryOutput.h"
#include "DataTypes.h"
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

using namespace pdb;
int main(int argc, char* argv[]) {


    bool printResult = true;
    bool clusterMode = false;
    if (argc > 1) {

        printResult = false;
        std::cout << "You successfully disabled printing result." << std::endl;

    } else {
        std::cout << "Will print result. If you don't want to print result, you can add any "
                     "character as the first parameter to disable result printing."
                  << std::endl;
    }

    if (argc > 2) {

        clusterMode = true;
        std::cout << "You successfully set the test to run on cluster." << std::endl;

    } else {
        std::cout << "Will run on local node. If you want to run on cluster, you can add any "
                     "character as the second parameter to run on the cluster configured by "
                     "$PDB_HOME/conf/serverlist."
                  << std::endl;
    }


    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

    // register this query class
    string errMsg;
    PDBLoggerPtr myLogger = make_shared<pdb::PDBLogger>("clientLog");
    StorageClient temp(8108, "localhost", myLogger);

    temp.registerType("libraries/libChrisSelection.so", errMsg);
    temp.registerType("libraries/libStringSelection.so", errMsg);

    // connect to the query client
    QueryClient myClient(8108, "localhost", myLogger, true);

    PDBLoggerPtr logger = make_shared<PDBLogger>("client48.log");

    // make the query graph
    Handle<Set<SharedEmployee>> myInputSet =
        myClient.getSet<SharedEmployee>("chris_db", "chris_set");
    Handle<ChrisSelection> myFirstSelect = makeObject<ChrisSelection>();
    myFirstSelect->setInput(myInputSet);
    Handle<QueryOutput<String>> outputOne =
        makeObject<QueryOutput<String>>("chris_db", "output_set1", myFirstSelect);


    auto begin = std::chrono::high_resolution_clock::now();

    if (!myClient.execute(errMsg, outputOne)) {
        std::cout << "Query failed. Message was: " << errMsg << "\n";
        return 0;
    }
    std::cout << std::endl;

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Time Duration: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

    std::cout << std::endl;
    // print the resuts

    if ((printResult == true) && (clusterMode == false)) {
        SetIterator<String> result = myClient.getSetIterator<String>("chris_db", "output_set1");
        std::cout << "First set of query results: ";
        for (auto a : result) {
            std::cout << (*a) << "; ";
        }
        std::cout << "\n\nSecond set of query results: ";
        result = myClient.getSetIterator<String>("chris_db", "output_set2");
        for (auto a : result) {
            std::cout << (*a) << "; ";
        }
        std::cout << "\n";
    }

    if (clusterMode == false) {
        // and delete the sets
        myClient.deleteSet("chris_db", "output_set1");
        myClient.deleteSet("chris_db", "output_set2");
    }
    system("scripts/cleanupSoFiles.sh");
}

#endif
