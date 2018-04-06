#ifndef RUN_QUERY17_CC
#define RUN_QUERY17_CC

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
#include "Query17.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>


using namespace std;
using namespace tpch;

/*
    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === "Brand#23" && $"p_container" === "MED BOX")
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)

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
        pdbClient.registerType ("libraries/libQ17JoinedTPCHPartTPCHLineItem.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemAvgJoin.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartSelection.so");
        pdbClient.registerType ("libraries/libQ17TPCHLineItemAvgQuantity.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemJoin.so");
        pdbClient.registerType ("libraries/libQ17PriceSum.so");
        pdbClient.registerType ("libraries/libQ17TPCHPartTPCHLineItemIdentitySelection.so");
    }    


    // now, create the sets for storing TPCHCustomer Data
    pdbClient.removeSet("tpch", "q17_output_set");
    if (!pdbClient.createSet<DoubleSumResult>(
            "tpch", "q17_output_set")) {
        cout << "Not able to create set. " ;
        exit(-1);
    } else {
        cout << "Created set.\n";
    }

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 256};

    // make the query graph
    Handle<Computation> myTPCHPartScanner = makeObject<ScanUserSet<TPCHPart>>("tpch", "part");
    Handle<Computation> myTPCHLineItemScanner = makeObject<ScanUserSet<TPCHLineItem>>("tpch", "lineitem");

    Handle<Computation> myQ17TPCHPartSelection = makeObject<Q17TPCHPartSelection>("Brand#23", "MED BOX");
    Handle<Computation> myQ17TPCHPartTPCHLineItemJoin = makeObject<Q17TPCHPartTPCHLineItemJoin>();
    Handle<Computation> myQ17TPCHPartTPCHLineItemIdentitySelection = 
         makeObject<Q17TPCHPartTPCHLineItemIdentitySelection> ();
    Handle<Computation> myQ17TPCHLineItemAvgQuantity = makeObject<Q17TPCHLineItemAvgQuantity>();
    Handle<Computation> myQ17TPCHPartTPCHLineItemAvgJoin = makeObject<Q17TPCHPartTPCHLineItemAvgJoin>();
    Handle<Computation> myQ17PriceSum = makeObject<Q17PriceSum>();
    Handle<Computation> myWriteSet = makeObject<WriteUserSet<DoubleSumResult>>("tpch", "q17_output_set");

    myQ17TPCHPartSelection->setInput(myTPCHPartScanner);
    myQ17TPCHPartTPCHLineItemJoin->setInput(0, myQ17TPCHPartSelection);
    myQ17TPCHPartTPCHLineItemJoin->setInput(1, myTPCHLineItemScanner);
    myQ17TPCHPartTPCHLineItemIdentitySelection->setInput(myQ17TPCHPartTPCHLineItemJoin);
    myQ17TPCHLineItemAvgQuantity->setInput(myQ17TPCHPartTPCHLineItemIdentitySelection);
    myQ17TPCHPartTPCHLineItemAvgJoin->setInput(0, myQ17TPCHPartTPCHLineItemIdentitySelection);
    myQ17TPCHPartTPCHLineItemAvgJoin->setInput(1, myQ17TPCHLineItemAvgQuantity);
    myQ17PriceSum->setInput(myQ17TPCHPartTPCHLineItemAvgJoin);
    myWriteSet->setInput(myQ17PriceSum);

    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    if (!pdbClient.executeComputations(myWriteSet)) {
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


    SetIterator<DoubleSumResult> result =
            pdbClient.getSetIterator<DoubleSumResult>("tpch", "q17_output_set");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        DoubleSumResult r = *a;
        std::cout << "sum is " << r.total << std::endl;
        count++;
    }
    std::cout << "Output count:" << count << "\n";
    std::cout << "#TimeDuration for query execution: " << timeDifference << " Second " << std::endl;

    // Remove the output set
    if (!pdbClient.removeSet("tpch", "q17_output_set")) {
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
