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
#ifndef RUN_QUERY22_CC
#define RUN_QUERY22_CC

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
#include "Query22.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace std;
using namespace tpch;

/*
    val sub2 = udf { (x: String) => x.substring(0, 2) }
    val phone = udf { (x: String) => x.matches("13|31|23|29|30|18|17") }
    val isNull = udf { (x: Any) => println(x); true }

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
      .filter(phone($"cntrycode"))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      //.filter("o_custkey is null")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")
*/


int main(int argc, char* argv[]) {

    bool whetherToRegisterLibraries = false;
    if (argc > 1) {
        if (strcmp(argv[1], "Y") == 0) {
            whetherToRegisterLibraries = true;
        }
    }


    // Connection info
    string masterHostname = "localhost";
    int masterPort = 8108;

    // register the shared employee class
    pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

    PDBClient pdbClient(
            masterPort, masterHostname);


    if (whetherToRegisterLibraries == true) {
        pdbClient.registerType("libraries/libQ22AggregatedCntryBal.so");
        pdbClient.registerType("libraries/libQ22TPCHCustomerAccbalAvg.so");
        pdbClient.registerType("libraries/libQ22TPCHOrderCountSelection.so");
        pdbClient.registerType("libraries/libQ22CntryBalAgg.so");
        pdbClient.registerType("libraries/libQ22JoinedCntryBal.so");
        pdbClient.registerType("libraries/libQ22CntryBalJoin.so");
        pdbClient.registerType("libraries/libQ22TPCHOrderCountPerTPCHCustomer.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q22_output_set");
    if (!pdbClient.createSet<DoubleSumResult>(
            "tpch", "q22_output_set")) {
        cout << "Not able to create set. " ;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    Handle<Computation> myTPCHCustomerScanner = makeObject<ScanUserSet<TPCHCustomer>>("tpch", "customer");
    Handle<Computation> myTPCHOrderScanner = makeObject<ScanUserSet<TPCHOrder>>("tpch", "order");
    Handle<Computation> myQ22TPCHCustomerAccbalAvg = makeObject<Q22TPCHCustomerAccbalAvg>("13", "31", "23", "29", "30", "18", "17");
    Handle<Computation> myQ22TPCHOrderCountPerTPCHCustomer = makeObject<Q22TPCHOrderCountPerTPCHCustomer>();
    Handle<Computation> myQ22TPCHOrderCountSelection = 
         makeObject<Q22TPCHOrderCountSelection> ();
    Handle<Computation> myQ22CntryBalJoin = makeObject<Q22CntryBalJoin>();
    Handle<Computation> myQ22CntryBalAgg = makeObject<Q22CntryBalAgg>();
    Handle<Computation> myWriteSet = makeObject<WriteUserSet<Q22AggregatedCntryBal>>("tpch", "q22_output_set");

    myQ22TPCHCustomerAccbalAvg->setInput(myTPCHCustomerScanner);
    myQ22TPCHOrderCountPerTPCHCustomer->setInput(myTPCHOrderScanner);
    myQ22TPCHOrderCountSelection->setInput(myQ22TPCHOrderCountPerTPCHCustomer);
    myQ22CntryBalJoin->setInput(0, myQ22TPCHOrderCountSelection);
    myQ22CntryBalJoin->setInput(1, myTPCHCustomerScanner);
    myQ22CntryBalJoin->setInput(2, myQ22TPCHCustomerAccbalAvg);
    myQ22CntryBalAgg->setInput(myQ22CntryBalJoin);
    myWriteSet->setInput(myQ22CntryBalAgg);

    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myWriteSet)) {
        std::cout << "Query failed. "  << "\n";
        return 1;
    }

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;



    // Printing results to double check
    std::cout << "to print result..." << std::endl;


    SetIterator<Q22AggregatedCntryBal> result =
            pdbClient.getSetIterator<Q22AggregatedCntryBal>("tpch", "q22_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        Q22AggregatedCntryBal r = *a;
        std::cout << r.cntrycode << ":" << r.avg.total << "," << r.avg.count << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q22_output_set")) {
        cout << "Not able to remove the set. ";
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
