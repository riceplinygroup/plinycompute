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
#ifndef TPCH_REGISTER_AND_CREATE_SETS_CC
#define TPCH_REGISTER_AND_CREATE_SETS_CC

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

#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"
#include "SumResultWriteSet.h"
#include "CustomerWriteSet.h"
#include "CustomerSupplierPartGroupBy.h"
#include "CustomerMultiSelection.h"
#include "ScanCustomerSet.h"
#include "SupplierData.h"
#include "CountAggregation.h"
#include "SumResult.h"

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


#define KB 1024
#define MB (1024 * KB)
#define GB (1024 * MB)

#define BLOCKSIZE (256 * MB)

int main(int argc, char* argv[]) {

    // Connection info
    string masterHostname = "localhost";
    int masterPort = 8108;

    // register the shared employee class
    PDBClient pdbClient(masterPort, masterHostname);

    string errMsg;

    cout << "Register Types Part, Supplier, LineItem, Order, Customer \n";

    pdbClient.registerType("libraries/libPart.so");
    pdbClient.registerType("libraries/libSupplier.so");
    pdbClient.registerType("libraries/libLineItem.so");
    pdbClient.registerType("libraries/libOrder.so");
    pdbClient.registerType("libraries/libCustomer.so");

    // now, create a new database
    pdbClient.createDatabase("TPCH_db");

    // now, create the sets for storing Customer Data
    pdbClient.createSet<Customer>(
            "TPCH_db", "tpch_bench_set1");

    cout << "Register further Types ... \n";

    pdbClient.registerType("libraries/libSumResultWriteSet.so");
    pdbClient.registerType("libraries/libCustomerWriteSet.so");
    pdbClient.registerType("libraries/libScanCustomerSet.so");
    pdbClient.registerType("libraries/libCustomerMultiSelection.so");
    pdbClient.registerType("libraries/libCustomerSupplierPartGroupBy.so");
    pdbClient.registerType("libraries/libSupplierInfo.so");
    pdbClient.registerType("libraries/libCustomerSupplierPartFlat.so");
    pdbClient.registerType("libraries/libCountAggregation.so");
    pdbClient.registerType("libraries/libCountCustomer.so");
    pdbClient.registerType("libraries/libSupplierInfoWriteSet.so");

    cout << errMsg << endl;


    // Clean up the SO files.
    int code = system("scripts/cleanupSoFiles.sh");
    if (code < 0) {

        std::cout << "Can't cleanup so files" << std::endl;
    }
}

#endif
