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

#include "Part.h"
#include "Supplier.h"
#include "LineItem.h"
#include "Order.h"
#include "Customer.h"
#include "CustomerSupplierPartWriteSet.h"
#include "CustomerSupplierPart.h"
#include "CustomerMapSelection.h"
#include "CustomerSupplierPartGroupBy.h"
#include "ScanCustomerSet.h"

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

// #include "Set.h"
//TODO: why shoud I include WriteStringSet when I want to use DispatcherClient?
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

pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>dataGenerator(std::string scaleFactor) {

	// All files to parse:
	string PartFile = "tables_scale_" + scaleFactor + "/part.tbl";
	string supplierFile = "tables_scale_" + scaleFactor + "/supplier.tbl";
	string orderFile = "tables_scale_" + scaleFactor + "/orders.tbl";
	string lineitemFile = "tables_scale_" + scaleFactor + "/lineitem.tbl";
	string customerFile = "tables_scale_" + scaleFactor + "/customer.tbl";

	// Common constructs:
	string line;
	string delimiter = "|";
	ifstream infile;

	// ####################################
	// ####################################
	// ##########                 #########
	// ##########      Part       #########
	// ##########                 #########
	// ####################################
	// ####################################

	// Open "PartFile": Iteratively (Read line, Parse line, Create Objects):

	infile.open(PartFile.c_str());

	vector<pdb::Handle<Part>> partList;

	// load up the allocator with RAM

	// This does not work with 100 KB memory
	// But it works with 1 MB or more memory.
//	pdb::makeObjectAllocatorBlock((size_t) 1000 * MB, true);

//	pdb::Handle <pdb::Vector <pdb::Handle <Part>>> partList =  pdb::makeObject<pdb::Vector<pdb::Handle<Part>>> ();

	map<int, pdb::Handle<Part>> partMap;

	while (getline(infile, line)) {
		stringstream lineStream(line);
		std::vector<string> tokens;
		string token;

		while (getline(lineStream, token, '|')) {
			tokens.push_back(token);
		}

		int partID = atoi(tokens.at(0).c_str());

		if (partID % 10000 == 0)
		cout << "Part ID: " << partID << endl;

		pdb::Handle<Part> part = pdb::makeObject<Part>(atoi(tokens.at(0).c_str()), tokens.at(1), tokens.at(2), tokens.at(3), tokens.at(4), atoi(tokens.at(5).c_str()), tokens.at(6), atof(tokens.at(7).c_str()),
				tokens.at(8));

		partList.push_back(part);

		//Populate the hash:
		partMap[atoi(tokens.at(0).c_str())] = part;
	}

	infile.close();
	infile.clear();

	// ####################################
	// ####################################
	// ##########                 #########
	// ##########     Supplier    #########
	// ##########                 #########
	// ####################################
	// ####################################

	//Open "SupplierFile": Iteratively (Read line, Parse line, Create Objects):
	infile.open(supplierFile.c_str());
	vector<pdb::Handle<Supplier>> supplierList;
	map<int, pdb::Handle<Supplier>> supplierMap;

	while (getline(infile, line)) {
		stringstream lineStream(line);
		std::vector<string> tokens;
		string token;

		while (getline(lineStream, token, '|')) {
			tokens.push_back(token);
		}

		int supplierKey = atoi(tokens.at(0).c_str());

		pdb::Handle<Supplier> tSupplier = pdb::makeObject<Supplier>(supplierKey, tokens.at(1), tokens.at(2), atoi(tokens.at(3).c_str()), tokens.at(4), atof(tokens.at(5).c_str()), tokens.at(6));
		supplierList.push_back(tSupplier);

		if (supplierKey % 1000 == 0)
		cout << "Supplier ID: " << supplierKey << endl;

		//Populate the hash:
		supplierMap[atoi(tokens.at(0).c_str())] = tSupplier;
	}

	infile.close();
	infile.clear();

	// ####################################
	// ####################################
	// ##########                 #########
	// ##########     LineItem    #########
	// ##########                 #########
	// ####################################
	// ####################################

	//Open "LineitemFile": Iteratively (Read line, Parse line, Create Objects):
	infile.open(lineitemFile.c_str());

	pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>>lineItemList = pdb::makeObject<pdb::Vector<pdb::Handle<LineItem>>>();

	map<int, pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>> >lineItemMap;

	while (getline(infile, line)) {
		stringstream lineStream(line);
		std::vector<string> tokens;
		string token;

		while (getline(lineStream, token, '|')) {
			tokens.push_back(token);
		}

		int orderKey = atoi(tokens.at(0).c_str());
		int partKey = atoi(tokens.at(1).c_str());
		int supplierKey = atoi(tokens.at(2).c_str());

		pdb::Handle<Part> tPart;
		pdb::Handle<Supplier> tSupplier;

		//Find the appropriate "Part"
		if (partMap.find(partKey) != partMap.end()) {
			tPart = partMap[partKey];
		} else {
			throw invalid_argument("There is no such Part.");
		}

		//Find the appropriate "Part"
		if (supplierMap.find(supplierKey) != supplierMap.end()) {
			tSupplier = supplierMap[supplierKey];
		} else {
			throw invalid_argument("There is no such Supplier.");
		}

		pdb::Handle<LineItem> tLineItem = pdb::makeObject<LineItem>(tokens.at(0), orderKey, tSupplier, tPart, atoi(tokens.at(3).c_str()), atof(tokens.at(4).c_str()), atof(tokens.at(5).c_str()),
				atof(tokens.at(6).c_str()), atof(tokens.at(7).c_str()), tokens.at(8), tokens.at(9), tokens.at(10), tokens.at(11), tokens.at(12), tokens.at(13), tokens.at(14), tokens.at(15));

		if (orderKey % 100000 == 0)
		cout << "LineItem ID: " << orderKey << endl;

		//Populate the hash:
		if (lineItemMap.find(orderKey) != lineItemMap.end()) {
			// the key already exists in the map
			lineItemMap[orderKey]->push_back(tLineItem);
		} else {
			// make a new vector
			pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>>lineItemList = pdb::makeObject<pdb::Vector<pdb::Handle<LineItem>>>();
			// push in the vector
			lineItemList->push_back(tLineItem);
			// put in the map
			lineItemMap[orderKey] = lineItemList;
		}

	}

	infile.close();
	infile.clear();

	// ####################################
	// ####################################
	// ########## 				  #########
	// ##########       Order     #########
	// ##########                 #########
	// ####################################
	// ####################################

	//Open "OrderFile": Iteratively (Read line, Parse line, Create Objects):
	infile.open(orderFile.c_str());

	map<int, pdb::Handle<pdb::Vector<pdb::Handle<Order>>> >orderMap;

	while (getline(infile, line)) {
		stringstream lineStream(line);
		std::vector<string> tokens;
		string token;

		while (getline(lineStream, token, '|')) {
			tokens.push_back(token);
		}

		int orderKey = atoi(tokens.at(0).c_str());
		int customerKey = atoi(tokens.at(1).c_str());

		//Sanity Check:
		if (lineItemMap.find(orderKey) == lineItemMap.end()) {
			throw invalid_argument("There is no such Order.");
		}

		pdb::Handle<Order> tOrder = pdb::makeObject<Order>(lineItemMap[orderKey], orderKey, customerKey, tokens.at(2), atof(tokens.at(3).c_str()), tokens.at(4), tokens.at(5), tokens.at(6), atoi(tokens.at(7).c_str()),
				tokens.at(8));

		if (orderKey % 100000 == 0)
		cout << "Order ID: " << orderKey << endl;

		//Populate the hash:
		if (orderMap.find(customerKey) != orderMap.end()) {
			orderMap[customerKey]->push_back(tOrder);
		} else {
			pdb::Handle<pdb::Vector<pdb::Handle<Order>>>orderList = pdb::makeObject<pdb::Vector<pdb::Handle<Order>>>();
			orderList -> push_back(tOrder);
			orderMap[customerKey] = orderList;
		}

	}

	infile.close();
	infile.clear();

	// ####################################
	// ####################################
	// ##########                 #########
	// ##########    Customers    #########
	// ##########                 #########
	// ####################################
	// ####################################

	//Open "CustomerFile": Iteratively (Read line, Parse line, Create Objects):
	infile.open(customerFile.c_str());

//	vector<pdb::Handle<Customer>> customerList;

	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>> storeMeCustomerList = pdb::makeObject<pdb::Vector<pdb::Handle<Customer>>>();

	while (getline(infile, line)) {
		stringstream lineStream(line);
		vector<string> tokens;
		string token;

		while (getline(lineStream, token, '|')) {
			tokens.push_back(token);
		}

		int customerKey = atoi(tokens.at(0).c_str());

		//Sanity: Deal with Customers without orders.
		if (orderMap.find(customerKey) == orderMap.end()) {
			pdb::Handle<pdb::Vector<pdb::Handle<Order>>>tOrderArray;
			orderMap[customerKey] = tOrderArray;
		}

		pdb::Handle<Customer> tCustomer = pdb::makeObject<Customer>(orderMap[customerKey], customerKey, tokens.at(1), tokens.at(2), atoi(tokens.at(3).c_str()), tokens.at(4), atof(tokens.at(5).c_str()), tokens.at(6),
				tokens.at(7));

		storeMeCustomerList->push_back(tCustomer);

		if (customerKey % 1000 == 0) {
			cout << "Customer Key: " << customerKey << endl;

		}

	}

	infile.close();
	infile.clear();

	return storeMeCustomerList;

}

pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>generateSmallDataset(int maxNoOfCustomers) {

	int maxPartsInEachLineItem = 4;
	int maxLineItemsInEachOrder = 4;
	int maxOrderssInEachCostomer = 4;

	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>> customers = pdb::makeObject<pdb::Vector<pdb::Handle<Customer>>> ();

	//4. Make Customers
	for (int customerID = 0; customerID < maxNoOfCustomers; ++customerID) {
		pdb::Handle<pdb::Vector<pdb::Handle<Order>>> orders = pdb::makeObject<pdb::Vector<pdb::Handle<Order>>> ();
		//3. Make Order
		for (int orderID = 0; orderID < maxOrderssInEachCostomer; ++orderID) {

			pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>> lineItems = pdb::makeObject<pdb::Vector<pdb::Handle<LineItem>>> ();
			//2.  Make LineItems
			for (int i = 0; i < maxLineItemsInEachOrder; ++i) {
				pdb::Handle<Part> part = pdb::makeObject<Part>(i, "Part-" + to_string(i), "mfgr", "Brand1", "type1", i, "Container1", 12.1, "Part Comment1");
				pdb::Handle<Supplier> supplier = pdb::makeObject<Supplier>(i, "Supplier-" + to_string(i), "address", i, "Phone1", 12.1, "Supplier Comment1");
				pdb::Handle<LineItem> lineItem = pdb::makeObject<LineItem>("Linetem-" + to_string(i), i, supplier, part, i, 12.1, 12.1, 12.1, 12.1, "ReturnFlag1", "lineStatus1", "shipDate", "commitDate", "receiptDate",
						"sgipingStruct", "shipMode1", "Comment1");
				lineItems->push_back(lineItem);
			}

			pdb::Handle<Order> order = pdb::makeObject<Order>(lineItems, orderID, 1, "orderStatus", 1, "orderDate", "OrderPriority", "clerk", 1, "Order Comment1");
			orders->push_back(order);
		}

		pdb::Handle<Customer> customer = pdb::makeObject<Customer>(orders, customerID, "customerName " + to_string(customerID), "address",1, "phone", 12.1, "mktsegment", "Customer Comment "+ to_string(customerID));
		customers->push_back(customer);
	}

	return customers;

}

int main() {

	int noOfCopies = 2;

	// Connection info
	string masterHostname = "localhost";
	int masterPort = 8108;

	// register the shared employee class
	pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

	pdb::DistributedStorageManagerClient distributedStorageManagerClient(masterPort, masterHostname, clientLogger);
	pdb::CatalogClient catalogClient(masterPort, masterHostname, clientLogger);
	pdb::DispatcherClient dispatcherClient = DispatcherClient(masterPort, masterHostname, clientLogger);
	pdb::QueryClient queryClient(masterPort, masterHostname, clientLogger, true);

//	catalogClient.cleanup();

	string errMsg;
	if (!catalogClient.registerType("libraries/libPart.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libSupplier.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libLineItem.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libOrder.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libCustomer.so", errMsg))
		cout << "Not able to register type.\n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPart.so", errMsg))
		cout << "Not able to register type.\n";

	// now, create a new database
	if (!distributedStorageManagerClient.createDatabase("TPCH_db", errMsg)) {
		cout << "Not able to create database: " + errMsg;
		exit(-1);
	} else {
		cout << "Created database.\n";
	}

	// now, create the sets for storing Customer Data
	if (!distributedStorageManagerClient.createSet<Customer>("TPCH_db", "tpch_bench_set1", errMsg)) {
		cout << "Not able to create set: " + errMsg;
		exit(-1);
	} else {
		cout << "Created set.\n";
	}

	pdb::makeObjectAllocatorBlock((size_t) 2 * GB, true);

	//
	// Generate the data
	// TPCH Data file scale - Data should be in folder named "tables_scale_"+"scaleFactor"
	string scaleFactor = "0.1";
//	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>  storeMeCustomerList = dataGenerator(scaleFactor);
	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>storeMeCustomerList = generateSmallDataset(4);

	pdb::Record<Vector<Handle<Customer>>>*myBytes = getRecord <Vector <Handle <Customer>>> (storeMeCustomerList);
	size_t sizeOfCustomers = myBytes->numBytes();
	cout << "Size of Customer Vector is: " << sizeOfCustomers << endl;

	// store copies of the same dataset.
	for (int i = 1; i <= noOfCopies; ++i) {
		cout << "Storing Vector of Customers - Copy Number : " << i << endl;

		if (!dispatcherClient.sendData<Customer>(std::pair<std::string, std::string>("tpch_bench_set1", "TPCH_db"), storeMeCustomerList, errMsg)) {
			std::cout << "Failed to send data to dispatcher server" << std::endl;
			return -1;
		}
	}
	// flush to disk
//	distributedStorageManagerClient.flushData(errMsg);


//	if (!catalogClient.registerType("libraries/libOrderMultiSelection.so", errMsg))
//		cout << "Not able to register type  libOrderMultiSelection.\n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartWriteSet.so", errMsg))
		cout << "Not able to register type libOrderWriteSet.\n";

	if (!catalogClient.registerType("libraries/libScanCustomerSet.so", errMsg))
		cout << "Not able to register type libScanCustomerSet. \n";

	if (!catalogClient.registerType("libraries/libCustomerMapSelection.so", errMsg))
		cout << "Not able to register type libCustomerMapSelection. \n";

	if (!catalogClient.registerType("libraries/libCustomerSupplierPartGroupBy.so", errMsg))
		cout << "Not able to register type libCustomerSupplierPartGroupBy.\n";


	// now, create the sets for storing Customer Data
	if (!distributedStorageManagerClient.createSet<CustomerSupplierPart>("TPCH_db", "t_output_se1", errMsg)) {
		cout << "Not able to create set: " + errMsg;
		exit(-1);
	} else {
		cout << "Created set.\n";
	}

	// for allocations
	const UseTemporaryAllocationBlock tempBlock { (size_t) 128 * MB };

	// make the query graph
	Handle<Computation> myScanSet = makeObject<ScanCustomerSet>("TPCH_db", "tpch_bench_set1");

	Handle<Computation> myFlatten = makeObject<CustomerMapSelection>();
	myFlatten->setInput(myScanSet);

	Handle<Computation> myGroupBy = makeObject<CustomerSupplierPartGroupBy>();
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
	SetIterator<CustomerSupplierPart> result = queryClient.getSetIterator<CustomerSupplierPart>("TPCH_db", "t_output_se1");

	std::cout << "Query results: ";
	int count = 0;
	for (auto a : result) {
		count++;

		cout<<"-------------" << endl;
//		if (count % 10 == 0) {
//			std::cout << count << std::endl;
//			std::cout <<"CustomerName: "  << a->getCustomerName()->c_str() << std::endl;
		a->print();
//		}
	}
	std::cout << "Output count:" << count << "\n";

	// Clean up the SO files.
	int code = system("scripts/cleanupSoFiles.sh");
	if (code < 0) {

		std::cout << "Can't cleanup so files" << std::endl;

	}

}

#endif

