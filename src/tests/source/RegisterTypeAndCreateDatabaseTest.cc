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
#ifndef TEST_46_CC
#define TEST_46_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "Employee.h"
#include "InterfaceFunctions.h"

#include "SharedEmployee.h"

/*
 * This test registers data types and create a database and a set in the database.
 */

int main(int argc, char * argv[]) {

	std::cout << "Make sure to run bin/test603 in a different window to provide a catalog/storage server.\n";

	// register the shared employee class
	pdb::StorageClient temp(8108, "localhost", make_shared<pdb::PDBLogger>("clientLog"), true);

	string errMsg;

	const string myFiles[] = { "libSharedEmployee.so", "libChrisSelection.so", "libStringSelection.so" };

	for (string tmp : myFiles) {
		const string fileName = "libraries/" + tmp;
		//Register selection type
		if (!temp.registerType(fileName, errMsg)) {
			cout << "Not able to register type: " + errMsg;
			return 0;
		} else {
			cout << "Registered type.\n";
		}
	}

	// now, create a new database
	if (!temp.createDatabase("chris_db", errMsg)) {
		cout << "Not able to create database: " + errMsg;
	} else {
		cout << "Created database.\n";
	}

	// now, create a new set in that database
	if (!temp.createSet<SharedEmployee>("chris_db", "chris_set", errMsg)) {
		cout << "Not able to create set: " + errMsg;
	} else {
		cout << "Created set.\n";
	}

}

#endif

