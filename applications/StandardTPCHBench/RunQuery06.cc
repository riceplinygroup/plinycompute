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
#ifndef RUN_QUERY06_CC
#define RUN_QUERY06_CC

#include "CatalogClient.h"

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <map>
#include <chrono>
#include <sstream>
#include <vector>
#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "PDBDebug.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "PDBClient.h"
#include "DataTypes.h"
#include "InterfaceFunctions.h"
#include "Handle.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"
#include "QueryOutput.h"
#include "ScanUserSet.h"
#include "WriteUserSet.h"

#include "TPCHSchema.h"
#include "Query06.h"
#include "DoubleSumResult.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace std;
using namespace tpch;

/*

select
sum(l_extendedprice*l_discount) as revenue
from
lineitem
where
l_shipdate >= date '[DATE]'
and l_shipdate < date '[DATE]' + interval '1' year
and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01 and l_quantity < [QUANTITY];

*/


int main(int argc, char* argv[]) {

    bool whetherToRegisterLibraries = false;
    if (argc > 1) {
        if (strcmp(argv[1], "Y") == 0) {
            whetherToRegisterLibraries = true;
        }
    }


    // Connection info
    string managerHostname = "localhost";
    int managerPort = 8108;

    // register the shared employee class
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    PDBClient pdbClient(
            managerPort, managerHostname);


    if (whetherToRegisterLibraries == true) {
        pdbClient.registerType ("libraries/libQ06Agg.so");
        pdbClient.registerType ("libraries/libQ06TPCHLineItemSelection.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q06_output_set");
    if (!pdbClient.createSet<DoubleSumResult>(
            "tpch", "q06_output_set")) {
        cout << "Not able to create set";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");
    Handle<Computation> myQ06TPCHLineItemSelection = makeObject<Q06TPCHLineItemSelection>();
    Handle<Computation> myQ06Agg = makeObject<Q06Agg>();
    Handle<Computation> myQ06Writer = makeObject<WriteUserSet<DoubleSumResult>> ("tpch", "q06_output_set");
    myQ06TPCHLineItemSelection->setInput(myTPCHLineItemScanner);
    myQ06Agg->setInput(myQ06TPCHLineItemSelection);
    myQ06Writer->setInput(myQ06Agg);


    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ06Writer)) {
        std::cout << "Query failed.  " << "\n";
        return 1;
    }

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;



    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<DoubleSumResult> result =
            pdbClient.getSetIterator<DoubleSumResult>("tpch", "q06_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        std::cout << a->total << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q06_output_set")) {
        cout << "Not able to remove the set";
        exit(-1);
    } else {
        cout << "Set removed. \n";
    }

    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {

        std::cout << "Can't cleanup so files" << std::endl;
    }

}
#endif
