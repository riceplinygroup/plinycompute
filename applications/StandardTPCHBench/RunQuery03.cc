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
#ifndef RUN_QUERY03_CC
#define RUN_QUERY03_CC

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
#include "Query03.h"

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
l_orderkey,
sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate,
o_shippriority
from
customer,
orders,
lineitem where
c_mktsegment = '[SEGMENT]' and c_custkey = o_custkey
and l_orderkey = o_orderkey and o_orderdate < date '[DATE]' and l_shipdate > date '[DATE]'
group by l_orderkey,
o_orderdate,
o_shippriority order by
revenue desc, o_orderdate;
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
        pdbClient.registerType ("libraries/libQ03Agg.so");
        pdbClient.registerType ("libraries/libQ03AggOut.so");
        pdbClient.registerType ("libraries/libQ03Join.so");
        pdbClient.registerType ("libraries/libQ03JoinOut.so");
        pdbClient.registerType ("libraries/libQ03TPCHCustomerSelection.so");
        pdbClient.registerType ("libraries/libQ03TPCHOrderSelection.so");
        pdbClient.registerType ("libraries/libQ03TPCHLineItemSelection.so");
        pdbClient.registerType ("libraries/libQ03KeyClass.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q03_output_set");
    if (!pdbClient.createSet<Q03AggOut>(
            "tpch", "q03_output_set")) {
        cout << "Not able to create set";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    Handle<Computation> myTPCHCustomerScanner = makeObject<ScanUserSet<TPCHCustomer>>("tpch", "customer");

    Handle<Computation> myTPCHOrderScanner = makeObject<ScanUserSet<TPCHOrder>>("tpch", "order");
    Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");
    Handle<Computation> myQ03TPCHCustomerSelection = makeObject<Q03TPCHCustomerSelection>("BUILDING");
    Handle<Computation> myQ03TPCHOrderSelection = makeObject<Q03TPCHOrderSelection>("1995-03-15");
    Handle<Computation> myQ03TPCHLineItemSelection = makeObject<Q03TPCHLineItemSelection>("1995-03-15");
    Handle<Computation> myQ03Join = makeObject<Q03Join>();
    Handle<Computation> myQ03Agg = makeObject<Q03Agg>();
    Handle<Computation> myQ03Writer = makeObject<WriteUserSet<Q03AggOut>> ("tpch", "q03_output_set");
    myQ03TPCHCustomerSelection->setInput(myTPCHCustomerScanner);
    myQ03TPCHOrderSelection->setInput(myTPCHOrderScanner);
    myQ03TPCHLineItemSelection->setInput(myTPCHLineItemScanner);
    myQ03Join->setInput(0, myQ03TPCHCustomerSelection);
    myQ03Join->setInput(1, myQ03TPCHOrderSelection);
    myQ03Join->setInput(2, myQ03TPCHLineItemSelection);
    myQ03Agg->setInput(myQ03Join);
    myQ03Writer->setInput(myQ03Agg);


    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ03Writer)) {
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


    SetIterator<Q03AggOut> result =
            pdbClient.getSetIterator<Q03AggOut>("tpch", "q03_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        double r = a->getValue();
        std::cout << "r=" << r << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q03_output_set")) {
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
