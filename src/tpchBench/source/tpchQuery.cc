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
#ifndef TPCH_DATA_GENERATOR_CC
#define TPCH_DATA_GENERATOR_CC

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
#include "SupplierData.h"

#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"
#include "CustomerSupplierPartWriteSet.h"
#include "CustomerSupplierPartGroupBy.h"
#include "CustomerMultiSelection.h"
#include "ScanCustomerSet.h"
#include "SupplierData.h"

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

//TODO: why should I include WriteStringSet when I want to use DispatcherClient?
#include "WriteStringSet.h"

using namespace std;

// Run a Cluster on Localhost
// ./bin/pdb-cluster localhost 8108 Y
// ./bin/pdb-server 4 4000 localhost:8108 localhost:8109
// ./bin/pdb-server 4 4000 localhost:8108 localhost:8110

// TPCH data set is available here https://drive.google.com/file/d/0BxMSXpJqaNfNMzV1b1dUTzVqc28/view
// Just unzip the file and put the folder in main directory of PDB

#define KB 1024
#define MB (1024*KB)
#define GB (1024*MB)

int main() {

	int noOfCopies = 2;

	// Connection info
	string masterHostname = "localhost";
	int masterPort = 8108;

	// register the shared employee class
	pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

	pdb::DistributedStorageManagerClient distributedStorageManagerClient(masterPort, masterHostname, clientLogger);
	pdb::CatalogClient catalogClient(masterPort, masterHostname, clientLogger);
//	pdb::DispatcherClient dispatcherClient = DispatcherClient(masterPort, masterHostname, clientLogger);
	pdb::QueryClient queryClient(masterPort, masterHostname, clientLogger, true);

	string errMsg;
	if (!catalogClient.registerType("libraries/libCustomerSupplierPartWriteSet.so", errMsg))
		cout << "Not able to register type libOrderWriteSet.\n";

	if (!catalogClient.registerType("libraries/libScanCustomerSet.so", errMsg))
		cout << "Not able to register type libScanCustomerSet. \n";

	if (!catalogClient.registerType("libraries/libCustomerMapSelection.so", errMsg))
		cout << "Not able to register type libCustomerMapSelection. \n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartAgg.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartGroupBy.so", errMsg))
		cout << "Not able to register type libCustomerSupplierPartGroupBy.\n";

	if (!catalogClient.registerType("libraries/libSupplierPart.so", errMsg))
		cout << "Not able to register type.\n";

	// now, create the sets for storing Customer Data
		if (!distributedStorageManagerClient.createSet<SupplierData>("TPCH_db", "t_output_se1", errMsg)) {
			cout << "Not able to create set: " + errMsg;
			exit(-1);
		} else {
			cout << "Created set.\n";
		}

		// for allocations
	    const UseTemporaryAllocationBlock tempBlock {1024 * 1024 * 128};


		// make the query graph
		Handle<Computation> myScanSet = makeObject<ScanCustomerSet>("TPCH_db", "tpch_bench_set1");

		Handle<Computation> myFlatten = makeObject<CustomerMultiSelection>();
		myFlatten->setInput(myScanSet);

		Handle<Computation> myGroupBy = makeObject<CustomerSupplierPartGroupBy>();
	//	myGroupBy->setAllocatorPolicy(noReuseAllocator);
		myGroupBy->setInput(myFlatten);

		Handle<Computation> myWriteSet = makeObject<CustomerSupplierPartWriteSet>("TPCH_db", "t_output_se1");
		myWriteSet->setInput(myGroupBy);

		auto begin = std::chrono::high_resolution_clock::now();

		if (!queryClient.executeComputations(errMsg, myWriteSet)) {
			std::cout << "Query failed. Message was: " << errMsg << "\n";
			return 1;
		}

		std::cout << std::endl;
		auto end = std::chrono::high_resolution_clock::now();
		std::cout << "Time Duration: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns." << std::endl;

		std::cout << "to print result..." << std::endl;
		SetIterator<SupplierData> result = queryClient.getSetIterator<SupplierData>("TPCH_db", "t_output_se1");

		std::cout << "Query results: ";
		int count = 0;
		for (auto a : result) {
			count++;

	//		cout<<"-------------" << endl;
	//		if (count % 1000 == 0) {
	//			std::cout << count << std::endl;
	//			std::cout <<"CustomerName: "  << a->getName() << std::endl;
			    a->print();
	//		}
		}
		std::cout << "Output count:" << count << "\n";





		// Remove the output set
		if (!distributedStorageManagerClient.removeSet("TPCH_db", "t_output_se1", errMsg)) {
			cout << "Not able to remove the set: " + errMsg;
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
