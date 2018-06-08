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
#ifndef RUN_QUERY14_CC
#define RUN_QUERY14_CC

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
#include "Query14.h"

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
100.00 * sum(case
when p_type like 'PROMO%'
then l_extendedprice*(1-l_discount) else 0
end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from
lineitem,
part where
l_partkey = p_partkey
and l_shipdate >= date '[DATE]'
and l_shipdate < date '[DATE]' + interval '1' month;
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
        pdbClient.registerType ("libraries/libQ14Agg.so");
        pdbClient.registerType ("libraries/libQ14AggOut.so");
        pdbClient.registerType ("libraries/libQ14Join.so");
        pdbClient.registerType ("libraries/libQ14JoinOut.so");
        pdbClient.registerType ("libraries/libQ14ValueClass.so");
        pdbClient.registerType ("libraries/libQ14TPCHLineItemSelection.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q14_output_set");
    if (!pdbClient.createSet<Q14AggOut>(
            "tpch", "q14_output_set")) {
        cout << "Not able to create set. ";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    Handle<Computation> myTPCHPartScanner = makeObject<ScanUserSet<TPCHPart>>("tpch", "part");
    Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");
    Handle<Computation> myQ14TPCHLineItemSelection = makeObject<Q14TPCHLineItemSelection>();
    Handle<Computation> myQ14Join = makeObject<Q14Join>();
    Handle<Computation> myQ14Agg = makeObject<Q14Agg>();
    Handle<Computation> myQ14Writer = makeObject<WriteUserSet<Q14AggOut>> ("tpch", "q14_output_set");
    myQ14TPCHLineItemSelection->setInput(myTPCHLineItemScanner);
    myQ14Join->setInput(0, myQ14TPCHLineItemSelection);
    myQ14Join->setInput(1, myTPCHPartScanner);
    myQ14Agg->setInput(myQ14Join);
    myQ14Writer->setInput(myQ14Agg);


    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ14Writer)) {
        std::cout << "Query failed. " << "\n";
        return 1;
    }

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;



    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<Q14AggOut> result =
            pdbClient.getSetIterator<Q14AggOut>("tpch", "q14_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        std::cout << a->value.getPromoRevenue() << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q14_output_set")) {
        cout << "Not able to remove the set.";
        exit(-1);
    } else {
        cout << "Set removed. \n";
    }

    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh force");
    if (code < 0) {

        std::cout << "Can't cleanup so files" << std::endl;
    }

}
#endif
