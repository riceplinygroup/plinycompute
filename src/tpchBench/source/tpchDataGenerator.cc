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
#include "CustomerMultiSelection.h"
#include "OrderWriteSet.h"
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

#include "../headers/ScanCustomerSet.h"
// #include "Set.h"
//TODO: why shoud I include WriteStringSet when I want to use DispatcherClient?
#include "WriteStringSet.h"

using namespace std;

// Run a Cluster on Localhost
// ./bin/pdb-cluster localhost 8108 Y
// ./bin/pdb-server 1 512 localhost:8108 localhost:8109
// ./bin/pdb-server 1 512 localhost:8108 localhost:8110

// TPCH data set is available here https://drive.google.com/file/d/0BxMSXpJqaNfNMzV1b1dUTzVqc28/view
// Just unzip the file and put the folder in main directory of PDB

#define KB 1024
#define MB (1024*KB)

pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>dataGenerator(std::string scaleFactor) {

	// All files to parse:
	string PartFile = "tables_scale_" + scaleFactor + "/part.tbl";
	string SupplierFile = "tables_scale_" + scaleFactor + "/supplier.tbl";
	string OrderFile = "tables_scale_" + scaleFactor + "/orders.tbl";
	string LineitemFile = "tables_scale_" + scaleFactor + "/lineitem.tbl";
	string CustomerFile = "tables_scale_" + scaleFactor + "/customer.tbl";

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
	pdb::makeObjectAllocatorBlock((size_t) 9000 * MB, true);

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
	infile.open(SupplierFile.c_str());
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
	infile.open(LineitemFile.c_str());

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
	infile.open(OrderFile.c_str());

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
	// ########## #########
	// ########## Customers #########
	// ########## #########
	// ####################################
	// ####################################

	//Open "CustomerFile": Iteratively (Read line, Parse line, Create Objects):
	infile.open(CustomerFile.c_str());

//	vector<pdb::Handle<Customer>> customerList;

	pdb::makeObjectAllocatorBlock((size_t) 15000 * MB, true);

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

pdb::Handle<pdb::Vector<pdb::Handle<Customer>>> generateSmallDataset() {


	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>> customers = pdb::makeObject<pdb::Vector<pdb::Handle<Customer>>> ();

	int maxPartsInEachLineItem = 10;
	int maxLineItemsInEachOrder = 20;
	int maxOrderssInEachCostomer = 20;

	// Make Customers
	for (int customerID = 0; customerID < 10; ++customerID) {

		pdb::Handle<pdb::Vector<pdb::Handle<Order>>>  orders = pdb::makeObject<pdb::Vector<pdb::Handle<Order>>> ();
		pdb::makeObjectAllocatorBlock((size_t) 100 * MB, true);

		// Make LineItems
		for (int lineItemID = 0; lineItemID < maxLineItemsInEachOrder; ++lineItemID) {

			pdb::Handle<pdb::Vector<pdb::Handle<LineItem>>>lineItems = pdb::makeObject<pdb::Vector<pdb::Handle<LineItem>>> ();

			for (int partID = 0; partID < maxPartsInEachLineItem; ++partID) {
				//1.  Make Part and Supplier
				pdb::Handle<Part> part = pdb::makeObject<Part>(partID, "Part1", "mfgr", "Brand1", "type1", partID, "Container1", 12.1, "Comment1");
				pdb::Handle<Supplier> supplier = pdb::makeObject<Supplier>(partID, "Part1", "address", partID, "Phone1", 12.1, "Comment1");

				//2. Make LineItem
				pdb::Handle<LineItem> lineItem = pdb::makeObject<LineItem>("Linetem1", partID, supplier, part, partID, 12.1, 12.1, 12.1, 12.1, "ReturnFlag1", "lineStatus1", "shipDate", "commitDate", "receiptDate",
						"sgipingStruct", "shipMode1", "Comment1");

				//3. Add the LineItem to the LineItem Vector
				lineItems->push_back(lineItem);
			}

			//4. Make Order
			pdb::Handle<Order> order = pdb::makeObject<Order>(lineItems, lineItemID, 1, "orderStatus", 1, "orderDate", "OrderPriority", "clerk", 1, "Comment1");
			orders->push_back(order);
		}

		pdb::Handle<Customer> customer = pdb::makeObject<Customer>(orders, customerID, "customerName", "address",1, "phone", 12.1,"mktsegment", "Comment1");
		customers->push_back(customer);
	}

	return customers;

}
int main() {

	int noOfCopies = 0;

	// Connection info
	string masterHostname = "localhost";
	int masterPort = 8108;

	// register the shared employee class
	pdb::PDBLoggerPtr clientLogger = make_shared<pdb::PDBLogger>("clientLog");

	pdb::DistributedStorageManagerClient distributedStorageManagerClient(masterPort, masterHostname, clientLogger);
	pdb::CatalogClient catalogClient(masterPort, masterHostname, clientLogger);
	pdb::DispatcherClient dispatcherClient = DispatcherClient(masterPort, masterHostname, clientLogger);

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

	// Phase - 2:  READ and Store the data
	// TPCH Data file scale - Data should be in folder named "tables_scale_"+"scaleFactor"

	string scaleFactor = "0.2";
//	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>  storeMeCustomerList = dataGenerator(scaleFactor);
	pdb::Handle<pdb::Vector<pdb::Handle<Customer>>>  storeMeCustomerList = generateSmallDataset();

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

		// flush to disk
//			distributedStorageManagerClient.flushData(errMsg);

	}

	if (!catalogClient.registerType("libraries/libCustomerMultiSelection.so", errMsg))
		cout << "Not able to register type libCustomerMultiSelection.\n";

	if (!catalogClient.registerType("libraries/libOrderMultiSelection.so", errMsg))
		cout << "Not able to register type  libOrderMultiSelection.\n";

	if (!catalogClient.registerType("libraries/libOrderWriteSet.so", errMsg))
		cout << "Not able to register type libOrderWriteSet.\n";

	if (!catalogClient.registerType("libraries/libScanCustomerSet.so", errMsg))
		cout << "Not able to register type libScanCustomerSet. \n";

	QueryClient myClient(masterPort, masterHostname, clientLogger, true);

//		std::cout << "to print result..." << std::endl;
//		SetIterator<Customer> result = myClient.getSetIterator<Customer>("TPCH_db", "tpch_bench_set1");
//
//		std::cout << "Query results: ";
//		int count = 0;
//		for (auto a : result) {
//			count++;
//			if (count % 10000 == 0) {
//				std::cout << count << endl;
//				cout<< " Customer Key" << a->getComment()->c_str() <<endl;
//			}
//		}
//		std::cout << "multi-selection output count:" << count << "\n";

	// for allocations
	const UseTemporaryAllocationBlock tempBlock { 1024 * 1024 * 128 };

	// make the query graph
	Handle<Computation> myScanSet = makeObject<ScanCustomerSet>("TPCH_db", "tpch_bench_set1");
	Handle<Computation> myFlatten = makeObject<CustomerMultiSelection>();
	myFlatten->setInput(myScanSet);
	Handle<Computation> myWriteSet = makeObject<OrderWriteSet>("TPCH_db", "output_set1");
	myWriteSet->setInput(myFlatten);

	auto begin = std::chrono::high_resolution_clock::now();

	if (!myClient.executeComputations(errMsg, myFlatten)) {
		std::cout << "Query failed. Message was: " << errMsg << "\n";
		return 1;
	}
	std::cout << std::endl;

	auto end = std::chrono::high_resolution_clock::now();
	std::cout << "Time Duration: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns." << std::endl;

}

#endif

