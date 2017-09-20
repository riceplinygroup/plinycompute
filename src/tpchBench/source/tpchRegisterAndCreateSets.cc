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
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
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
#include "QueryClient.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
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
#define MB (1024*KB)
#define GB (1024*MB)

#define BLOCKSIZE (256*MB)

int main(int argc, char * argv[]) {

	// Connection info
	string masterHostname = "localhost";
	int masterPort = 8108;

	// register the shared employee class
	pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

	pdb::DistributedStorageManagerClient distributedStorageManagerClient(masterPort, masterHostname, clientLogger);
	pdb::CatalogClient catalogClient(masterPort, masterHostname, clientLogger);

	string errMsg;

	cout << "Register Types Part, Supplier, LineItem, Order, Customer \n";
	if (!catalogClient.registerType("libraries/libPart.so", errMsg))
		cout << "Not able to register libPart type.\n";

	if (!catalogClient.registerType("libraries/libSupplier.so", errMsg))
		cout << "Not able to register libSupplier type.\n";

	if (!catalogClient.registerType("libraries/libLineItem.so", errMsg))
		cout << "Not able to register libLineItem type.\n";

	if (!catalogClient.registerType("libraries/libOrder.so", errMsg))
		cout << "Not able to register libOrder type.\n";

	if (!catalogClient.registerType("libraries/libCustomer.so", errMsg))
		cout << "Not able to register libCustomer type.\n";

	cout << errMsg << endl;



	// now, create a new database
	if (!distributedStorageManagerClient.createDatabase("TPCH_db", errMsg)) {
		cout << "Not able to create database: " + errMsg;
		exit(-1);
	} else {
		cout << "Created TPCH_db database.\n";
	}

	// now, create the sets for storing Customer Data
	if (!distributedStorageManagerClient.createSet<Customer>("TPCH_db", "tpch_bench_set1", errMsg)) {
		cout << "Not able to create set: " + errMsg;
		exit(-1);
	} else {
		cout << "Created tpch_bench_set1  set.\n";
	}



	cout << "Register further Types ... \n";


	if (!catalogClient.registerType("libraries/libSumResultWriteSet.so", errMsg))
		cout << "Not able to register type libSumResultWriteSet.\n";

	if (!catalogClient.registerType("libraries/libCustomerWriteSet.so", errMsg))
		cout << "Not able to register type libCustomerWriteSet.\n";

	if (!catalogClient.registerType("libraries/libScanCustomerSet.so", errMsg))
		cout << "Not able to register type libScanCustomerSet. \n";

	if (!catalogClient.registerType("libraries/libCustomerMultiSelection.so", errMsg))
		cout << "Not able to register type libCustomerMapSelection. \n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartGroupBy.so", errMsg))
		cout << "Not able to register type libCustomerSupplierPartGroupBy.\n";

	if (!catalogClient.registerType("libraries/libSupplierData.so", errMsg))
		cout << "Not able to register type  libSupplierData\n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartFlat.so", errMsg))
		cout << "Not able to register type  libCustomerSupplierPartFlat\n";

	if (!catalogClient.registerType("libraries/libCountAggregation.so", errMsg))
		cout << "Not able to register type  libCountAggregation\n";

	if (!catalogClient.registerType("libraries/libCountCustomer.so", errMsg))
		cout << "Not able to register type  libCountCustomer\n";

        if (!catalogClient.registerType("libraries/libSupplierDataWriteSet.so", errMsg))
                cout << "Not able to register type libSupplierDataWriteSet\n";

	cout << errMsg << endl;





	// Clean up the SO files.
	int code = system("scripts/cleanupSoFiles.sh");
	if (code < 0) {

		std::cout << "Can't cleanup so files" << std::endl;

	}

}

#endif

