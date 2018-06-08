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
#ifndef RUN_QUERY02_CC
#define RUN_QUERY02_CC

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
#include "Query02.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace std;
using namespace tpch;

/*
    val europe = region.filter($"r_name" === "EUROPE")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"r_regionkey", $"n_regionkey", $"s_suppkey", $"n_nationkey", $"s_nationkey", $"p_partkey", $"p_mfgr", $"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === 15 && part("p_type").endsWith("BRASS"))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
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
        pdbClient.registerType ("libraries/libMinDouble.so");
        pdbClient.registerType ("libraries/libQ02MinAgg.so");
        pdbClient.registerType ("libraries/libQ02MinCostJoin.so");
        pdbClient.registerType ("libraries/libQ02MinCostJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02MinCostPerTPCHPart.so");
        pdbClient.registerType ("libraries/libQ02MinCostSelection.so");
        pdbClient.registerType ("libraries/libQ02MinCostSelectionOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHNationJoin.so"); 
        pdbClient.registerType ("libraries/libQ02TPCHPartJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHPartSelection.so");
        pdbClient.registerType ("libraries/libQ02TPCHTPCHPartSuppJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHTPCHPartSuppJoinOutput.so");
        pdbClient.registerType ("libraries/libQ02TPCHRegionSelection.so");
        pdbClient.registerType ("libraries/libQ02TPCHSupplierJoin.so");
        pdbClient.registerType ("libraries/libQ02TPCHSupplierJoinOutput.so");      
        pdbClient.registerType ("libraries/libQ02TPCHPartJoinOutputIdentitySelection.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q02_output_set");
    if (!pdbClient.createSet<Q02MinCostSelectionOutput>(
            "tpch", "q02_output_set")) {
        cout << "Not able to create set.";
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    //Handle<Computation> myTPCHCustomerScanner = makeObject<ScanUserSet<TPCHCustomer>>("tpch", "customer");
    //Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");
    Handle<Computation> myTPCHNationScanner = makeObject<ScanUserSet<TPCHNation>>("tpch", "nation");
    //Handle<Computation> myTPCHOrderScanner = makeObject<ScanUserSet<TPCHOrder>>("tpch", "order");
    Handle<Computation> myTPCHPartScanner = makeObject<ScanUserSet<TPCHPart>>("tpch", "part");
    Handle<Computation> myTPCHTPCHPartSuppScanner = makeObject<ScanUserSet<TPCHTPCHPartSupp>>("tpch", "partsupp");
    Handle<Computation> myTPCHRegionScanner = makeObject<ScanUserSet<TPCHRegion>>("tpch", "region");
    Handle<Computation> myTPCHSupplierScanner = makeObject<ScanUserSet<TPCHSupplier>>("tpch", "supplier");

    Handle<Computation> myQ02TPCHRegionSelection = makeObject<Q02TPCHRegionSelection>("EUROPE");
    Handle<Computation> myQ02TPCHNationJoin = makeObject<Q02TPCHNationJoin>();
    Handle<Computation> myQ02TPCHSupplierJoin = makeObject<Q02TPCHSupplierJoin>();
    Handle<Computation> myQ02TPCHTPCHPartSuppJoin = makeObject<Q02TPCHTPCHPartSuppJoin>();
    Handle<Computation> myQ02TPCHPartSelection = makeObject<Q02TPCHPartSelection>(15, "BRASS");
    Handle<Computation> myQ02TPCHPartJoin = makeObject<Q02TPCHPartJoin>();
    Handle<Computation> myQ02TPCHPartJoinOutputSelection = makeObject<Q02TPCHPartJoinOutputIdentitySelection>();
    Handle<Computation> myQ02MinAgg = makeObject<Q02MinAgg>();
    Handle<Computation> myQ02MinCostJoin = makeObject<Q02MinCostJoin>();
    Handle<Computation> myQ02MinCostSelection = makeObject<Q02MinCostSelection>();

    Handle<Computation> myQ02MinCostSelectionOutputWriter = makeObject<WriteUserSet<Q02MinCostSelectionOutput>> ("tpch", "q02_output_set");

    myQ02TPCHRegionSelection->setInput(myTPCHRegionScanner);
    myQ02TPCHNationJoin->setInput(0, myQ02TPCHRegionSelection);
    myQ02TPCHNationJoin->setInput(1, myTPCHNationScanner);
    myQ02TPCHSupplierJoin->setInput(0, myQ02TPCHNationJoin);
    myQ02TPCHSupplierJoin->setInput(1, myTPCHSupplierScanner);
    myQ02TPCHTPCHPartSuppJoin->setInput(0, myQ02TPCHSupplierJoin);
    myQ02TPCHTPCHPartSuppJoin->setInput(1, myTPCHTPCHPartSuppScanner);
    
    myQ02TPCHPartSelection->setInput(myTPCHPartScanner);
    myQ02TPCHPartJoin->setInput(0, myQ02TPCHPartSelection);
    myQ02TPCHPartJoin->setInput(1, myQ02TPCHTPCHPartSuppJoin);
    myQ02TPCHPartJoinOutputSelection->setInput(myQ02TPCHPartJoin);
    
    myQ02MinAgg->setInput(myQ02TPCHPartJoinOutputSelection);
    myQ02MinCostJoin->setInput(0, myQ02MinAgg);
    myQ02MinCostJoin->setInput(1, myQ02TPCHPartJoinOutputSelection);
    myQ02MinCostSelection->setInput(myQ02MinCostJoin);
    myQ02MinCostSelectionOutputWriter->setInput(myQ02MinCostSelection);


    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myQ02MinCostSelectionOutputWriter)) {
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


    SetIterator<Q02MinCostSelectionOutput> result =
            pdbClient.getSetIterator<Q02MinCostSelectionOutput>("tpch", "q02_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        Q02MinCostSelectionOutput r = *a;
        std::cout << "p_partkey=" << r.p_partkey << ", p_mfgr=" << r.p_mfgr << ", n_name=" << r.n_name 
            << ", s_acctbal=" << r.s_acctbal << ", s_name=" << r.s_name << ", s_address=" << r.s_address
            << ", s_phone=" << r.s_phone << ", s_comment=" << r.s_comment << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q02_output_set")) {
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
