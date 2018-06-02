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
#ifndef RUN_QUERY13_CC
#define RUN_QUERY13_CC

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
#include "Query13.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace std;
using namespace tpch;

/*
    val special = udf { (x: String) => x.matches(".*special.*requests.*") }

    customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
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
        pdbClient.registerType ("libraries/libQ13CountResult.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerTPCHOrderJoin.so");
        pdbClient.registerType ("libraries/libQ13TPCHOrderSelection.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerDistribution.so");
        pdbClient.registerType ("libraries/libQ13TPCHCustomerTPCHOrders.so");
        pdbClient.registerType ("libraries/libQ13TPCHOrdersPerTPCHCustomer.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q13_output_set");
    if (!pdbClient.createSet<Q13CountResult>(
            "tpch", "q13_output_set")) {
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

    Handle<Computation> myQ13TPCHOrderSelection = makeObject<Q13TPCHOrderSelection>("special", "request");
    Handle<Computation> myQ13TPCHCustomerTPCHOrderJoin = makeObject<Q13TPCHCustomerTPCHOrderJoin>();
    Handle<Computation> myQ13TPCHOrdersPerTPCHCustomer = makeObject<Q13TPCHOrdersPerTPCHCustomer>();
    Handle<Computation> myQ13TPCHCustomerDistribution = makeObject<Q13TPCHCustomerDistribution>();
    Handle<Computation> myWriteSet = makeObject<WriteUserSet<Q13CountResult>>("tpch", "q13_output_set");

    myQ13TPCHOrderSelection->setInput(myTPCHOrderScanner);
    myQ13TPCHCustomerTPCHOrderJoin->setInput(0, myTPCHCustomerScanner);
    myQ13TPCHCustomerTPCHOrderJoin->setInput(1, myQ13TPCHOrderSelection);
    myQ13TPCHOrdersPerTPCHCustomer->setInput(myQ13TPCHCustomerTPCHOrderJoin);
    myQ13TPCHCustomerDistribution->setInput(myQ13TPCHOrdersPerTPCHCustomer);
    myWriteSet->setInput(myQ13TPCHCustomerDistribution);

    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myWriteSet)) {
        std::cout << "Query failed." << "\n";
        return 1;
    }

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;



    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<Q13CountResult> result =
            pdbClient.getSetIterator<Q13CountResult>("tpch", "q13_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        Q13CountResult r = *a;
        std::cout << "there are " << r.value << " customers have " << r.key << " orders" << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q13_output_set")) {
        cout << "Not able to remove the set";
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
