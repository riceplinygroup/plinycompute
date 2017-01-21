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
/*
 * storeLotsOfEmployee.cc
 *
 *  Created on: Jan 19, 2017
 *      Author: kia
 */

#ifndef STORE_LOTS_OF_EMPLOYEE_CC
#define STORE_LOTS_OF_EMPLOYEE_CC

#include <cstddef>
#include <iostream>
#include <vector>
#include <chrono>
#include <ctime>
#include <string>

#include "StorageClient.h"
#include "PDBVector.h"
#include "Employee.h"
#include "InterfaceFunctions.h"

//#include "SharedEmployee.h"

#define KB 1024
#define MB (1024*KB)

using namespace pdb;

int main(int argc, char * argv[]) {

	cout << "make sure to have a server and run in another window  ./bin/test603 2 512" << endl;

	std::string databaseName("chris_db");
	std::string setName("chris_set");

	int numOfElementInsideTheObject = 10;
	int numOfObjects = 1000000;

	if (argc > 1) {
		numOfObjects = atoi(argv[1]);
	}
	string errMsg;

	// register the shared employee class
	pdb::StorageClient conn(8108, "localhost", make_shared<pdb::PDBLogger>("clientLog"), true);

	// now, create a new database
	if (!conn.createDatabase(databaseName, errMsg)) {
		std::cout << "Not able to create database: " << errMsg << endl;
	} else {
		std::cout << "Created database.\n";
	}

	// now create a new set in that database
	if (!conn.createSet<Employee>(databaseName, setName, errMsg)) {
		std::cout << "Not able to set: " << errMsg << endl;
	} else {
		std::cout << "Created set.\n";
	}

	// This does not work with 100 KB memory
	// But it works with 1 MB or more memory.
	pdb::makeObjectAllocatorBlock((size_t) 100 * KB, true);

	// for timing
	auto begin = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < numOfObjects; i++) {


		pdb::Handle<pdb::Vector<pdb::Handle<Employee>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle <Employee>>> ();

		try {
			pdb :: Handle <Employee> myData = pdb :: makeObject <Employee> ("Joe Johnson" + to_string (i), i + 45);
			storeMe->push_back (myData);

//			std :: cout << "In Allocator are currently " << getNumObjectsInCurrentAllocatorBlock () << " objects.\n";

			if(storeMe->size()!=0) {
				if (!conn.storeData <Employee> (storeMe, databaseName, setName, errMsg, false)) {
					cout << "Not able to store data: " + errMsg;
					return 0;
				}
			}

		} catch (pdb :: NotEnoughSpace &n) {
			// Because we store small size of data, this should never happen.
			std::cout << "This should never happen. NotEnoughSpace  " << endl;
		}


		if(i%100==0) {

			auto end = std::chrono::high_resolution_clock::now();


			std::cout << "Wrote Objects " << i ;
			std::cout << " in " <<
			std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count() << " milliseconds. " << endl;

			begin = std::chrono::high_resolution_clock::now();
		}
	}
}

#endif
