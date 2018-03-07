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
#ifndef TPCH_QUERY_ONLY_CC
#define TPCH_QUERY_ONLY_CC

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
#include "SupplierData.h"

#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"
#include "SumResultWriteSet.h"
#include "CustomerSupplierPartGroupBy.h"
#include "CustomerMultiSelection.h"
#include "ScanCustomerSet.h"
#include "SupplierData.h"
#include "CountAggregation.h"
#include "SumResult.h"
#include "SupplierInfoWriteSet.h"
#include "SupplierData.h"
#include "Handle.h"
#include "Lambda.h"
#include "LambdaCreationFunctions.h"
#include "UseTemporaryAllocationBlock.h"
#include "Pipeline.h"
#include "SelectionComp.h"
#include "WriteBuiltinEmployeeSet.h"
#include "SupervisorMultiSelection.h"
#include "VectorSink.h"
#include "HashSink.h"
#include "MapTupleSetIterator.h"
#include "VectorTupleSetIterator.h"
#include "ComputePlan.h"

#include "QueryOutput.h"
#include "DataTypes.h"

#include <ctime>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>

#include "WriteStringSet.h"

using namespace std;

// Run a Cluster on Localhost
// ./bin/pdb-cluster localhost 8108 Y
// ./bin/pdb-server 4 4000 localhost:8108 localhost:8109
// ./bin/pdb-server 4 4000 localhost:8108 localhost:8110

// TPCH data set is available here https://drive.google.com/file/d/0BxMSXpJqaNfNMzV1b1dUTzVqc28/view
// Just unzip the file and put the folder in main directory of PDB

#define KB 1024
#define MB (1024 * KB)
#define GB (1024 * MB)

int main() {

    int noOfCopies = 2;
    string errMsg;

    // Connection info
    string masterHostname = "localhost";
    int masterPort = 8108;

    // register the shared employee class
    PDBClient pdbClient(masterPort, masterHostname, false, true);

    // now, create the sets for storing Customer Data
    pdbClient.createSet<SumResult>(
            "TPCH_db", "t_output_set_1");

    // for allocations
    const UseTemporaryAllocationBlock tempBlock{1024 * 1024 * 128};

    // make the query graph
    Handle<Computation> myScanSet = makeObject<ScanCustomerSet>("TPCH_db", "tpch_bench_set1");

    Handle<Computation> myFlatten = makeObject<CustomerMultiSelection>();
    myFlatten->setInput(myScanSet);
    myFlatten->setAllocatorPolicy(noReuseAllocator);
    Handle<Computation> myGroupBy = makeObject<CustomerSupplierPartGroupBy>();
    myGroupBy->setInput(myFlatten);
    myGroupBy->setAllocatorPolicy(noReuseAllocator);
// Get the count by doing a count aggregation on the final results
#ifndef CHECK_RESULTS
    Handle<Computation> myWriteSet = makeObject<CountAggregation>("TPCH_db", "t_output_set_1");
    myWriteSet->setInput(myGroupBy);

// Handle<Computation> myWriteSet = makeObject<SumResultWriteSet>("TPCH_db", "t_output_set_1");
// myWriteSet->setInput(countAggregation);

#else

    Handle<Computation> myWriteSet = makeObject<SupplierInfoWriteSet>("TPCH_db", "t_output_set_1");
    myWriteSet->setInput(myGroupBy);

#endif

    // Query Execution and Time Calculation

    auto begin = std::chrono::high_resolution_clock::now();

    pdbClient.executeComputations(myWriteSet);

    std::cout << std::endl;
    auto end = std::chrono::high_resolution_clock::now();

    float timeDifference =
        (float(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count())) /
        (float)1000000000;

    std::cout << "#TimeDuration: " << timeDifference << " Second " << std::endl;


    // Printing results to double check
    std::cout << "to print result..." << std::endl;

#ifndef CHECK_RESULTS

    SetIterator<SumResult> result =
            pdbClient.getSetIterator<SumResult>("TPCH_db", "t_output_set_1");

    std::cout << "Query results: ";
    int count = 0;
    for (auto a : result) {
        count++;
        std::cout << "Total count is: " << a->total << std::endl;
    }
    std::cout << "Output count:" << count << "\n";

#else

    SetIterator<SupplierInfo> result =
        queryClient.getSetIterator<SupplierInfo>("TPCH_db", "t_output_set_1");
    std::cout << "Query results: ";
    int count = 0;
    long sum = 0;
    int min = 100000000;
    int max = 0;
    for (auto a : result) {
        count++;
        std::cout << "Supplier Name: " << a->getKey() << std::endl;
        int curNum = a->print();
        sum += curNum;
        if (curNum < min) {
            min = curNum;
        }
        if (curNum > max) {
            max = curNum;
        }
    }
    std::cout << "Output count: " << count << "\n";
    std::cout << "Average customers count for each supplier: " << sum / count << "\n";
    std::cout << "Max customers count for each supplier: " << max << "\n";
    std::cout << "Min customers count for each supplier: " << min << "\n";

#endif
    // Remove the output set
    pdbClient.removeSet("TPCH_db", "t_output_set_1");

    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {

        std::cout << "Can't cleanup so files" << std::endl;
    }

    const UseTemporaryAllocationBlock tempBlock2{1024 * 1024 * 128};
}
#endif
