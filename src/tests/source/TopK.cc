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

#ifndef TOP_K_CC
#define TOP_K_CC

#include "PDBDebug.h"
#include "TopKTest.h"
#include "EmpWithVector.h"
#include "ScanEmpWithVector.h"
#include "WriteEmpWithVector.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "DistributedStorageManagerClient.h"
#include "DispatcherClient.h"
#include "Set.h"

using namespace pdb;
int main (int argc, char * argv[]) {

	if (argc != 5) {
		std :: cout << "Usage: #dataSizeToAdd[MB] #masterIP #registerSharedLibs[Y/N] ";
		std :: cout << "#createDBAndSet[Y/N]\n";        
		return 0;
	}

	// here is how many MB to add
	unsigned numOfMb = atoi (argv[1]);
	std :: cout << "Will add " << numOfMb << " MB of data.\n";

	// the IP address of the master
	std :: string masterIP = argv[2];
	std :: cout << "IP address of master node is " << masterIP << "\n";
	
	// whether or not to add the shared libraries
	bool registerSharedLibs;
	if (strcmp (argv[3], "N") == 0) {
		registerSharedLibs = false;
		std :: cout << "I will not regsiter the shared libraries.\n";
	} else {
		registerSharedLibs = true;
		std :: cout << "And I will regsiter the shared libraries.\n";
	}

	// whether to create a new DB and set
	bool createNewDBAndSet;
	if (strcmp (argv[4], "N") == 0) {
		createNewDBAndSet = false;
		std :: cout << "I will not create a new database and data set.\n";
	} else {
		createNewDBAndSet = true;
		std :: cout << "And I will create a new database and data set.\n";
	}

	
	// for logging
	pdb :: PDBLoggerPtr clientLogger = make_shared<pdb :: PDBLogger>("clientLog");

	// for connecting to the catalog; necessary to use non-built-in PDB types
	pdb :: CatalogClient catalogClient (8108, masterIP, clientLogger);

	// if we register the types we are going to use to execute the query
	if (registerSharedLibs) {

		std :: string errMsg;
		catalogClient.registerType ("libraries/libEmpWithVector.so", errMsg);
		catalogClient.registerType ("libraries/libTopKTest.so", errMsg);
		catalogClient.registerType ("libraries/libScanEmpWithVector.so", errMsg);
		catalogClient.registerType ("libraries/libWriteEmpWithVector.so", errMsg);
	}

	// if we add data
	std :: string errMsg;

	// this will allow us to add data
	pdb :: DistributedStorageManagerClient client (8108, masterIP, clientLogger);
		
	if (numOfMb > 0) {

		if (createNewDBAndSet) {

			if (!client.createSet <EmpWithVector> ("topK_db", "topK_set", errMsg)) {
				cout << "Not able to create data set: " + errMsg;
				exit (-1);
			} else {
				cout << "Created data set.\n";
			}
		}
		
		DispatcherClient dispatcherClient = DispatcherClient (8108, masterIP, clientLogger);

		for (int i = 0; i < numOfMb; i++) {	

			// in each iteration, we will create a new allocation block
			pdb :: makeObjectAllocatorBlock (1024 * 1024, true);
			pdb::Handle <pdb::Vector <pdb::Handle <EmpWithVector>>> storeMe = pdb::makeObject <pdb::Vector <pdb::Handle <EmpWithVector>>> ();

			// now, fill it up
			int j = 0;
			while (true) {

				try {

					// try to create another EmpWithVector object
					Supervisor temp ("Joe Johnson", 20 + (j % 29));	
					for (int k = 0; k < 10; k++) {
						Handle <Employee> nextGuy = makeObject <Employee> ("Steve Stevens", 20 + ((j + k) % 29));
						temp.addEmp (nextGuy);	
					}
					Vector <double> myVec;
					for (int k = 0; k < 10; k++) {
						myVec.push_back (drand48 ());
					}
					pdb::Handle <EmpWithVector> addMe = makeObject <EmpWithVector> (temp, myVec);
					storeMe->push_back (addMe);
					j++;

				} catch (NotEnoughSpace &e) {

					// add the next MB of data
					if (!dispatcherClient.sendData<EmpWithVector> (std::pair <std::string, std::string>("topK_set", "topK_db"), storeMe, errMsg)) {
						std :: cout << "Failed to send data to dispatcher server" << std :: endl;
						return -1;
					} else {

						if (j == numOfMb) {
							break;
						}
						std :: cout << "Added " << j << " EmpWithVector objects to the database.\n";
					}
				}
			}
		}
	}

	// now we create the output set
	if (!client.createSet <TopKQueue <double, Handle <EmpWithVector>>> ("topK_db", "topKOutput_set", errMsg)) {
		cout << "Not able to create output data set: " + errMsg;
		exit (-1);
	}

	// for building the query
	pdb :: makeObjectAllocatorBlock (1024 * 1024, true);

	// here is the query vector
	Vector <double> query;
	for (int i = 0; i < 10; i++) {
		query.push_back (0.0);
	}

	// connect to the query client
	QueryClient myClient (8108, "localhost", clientLogger, true);

	// make the query graph
	Handle <Computation> myInitialScanSet = makeObject <ScanEmpWithVector> ("topK_db", "topK_set");
	Handle <Computation> myQuery = makeObject <TopKTest> (query);
	myQuery->setInput (myInitialScanSet);
	Handle <Computation> myWriter = makeObject <WriteEmpWithVector> ("topK_db", "topKOutput_set");
	myWriter->setInput (myQuery);

	// execute the query
        if (!myClient.executeComputations (errMsg, myWriter)) {
            std :: cout << "Query failed. Message was: " << errMsg << "\n";
            return 0;
        }

	// now iterate through the result
	SetIterator <TopKQueue <double, Handle <EmpWithVector>>> result = 
		myClient.getSetIterator <TopKQueue <double, Handle <EmpWithVector>>> ("topK_db", "topKOutput_set");
	for (auto &a : result) {
		int size = a->getScores ()->size ();
		std :: cout << "Got back " << size << " items from the top-k query.\n";
		std :: cout << "These items are:\n";
		for (int i = 0; i < size; i++) {
			std :: cout << "vector: ";
			for (int j = 0; j < a->getScores ()->size (); j++) {
				std :: cout << (*(a->getScores ()))[i] << " ";
			}
			std :: cout << "emp: " ;
			(*(a->getValues ()))[i]->getEmp ().print ();
			std :: cout << "\n";
		}
	}

	// now, remove the output set
	myClient.deleteSet ("topK_db", "topKOutput_set");
        int code = system ("scripts/cleanupSoFiles.sh");
        if (code < 0) {
            std :: cout << "Can't cleanup so files" << std :: endl;
        }
}

#endif
