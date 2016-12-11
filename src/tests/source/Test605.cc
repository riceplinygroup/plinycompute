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

#ifndef TEST_605_CC
#define TEST_605_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "Employee.h"
#include "Supervisor.h"
#include "InterfaceFunctions.h"

// this won't be visible to the v-table map, since it is not in the biult in types directory
#include "LeoQuery.h"

int main () {

	std:: cout << "First run bin/test603, then run this test in a different window.\n";
	std:: cout << "Then run bin/test603 again, then run bin/test606 in a different window.\n";

	// register the LeoQuery class
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), true);

	string errMsg;
	

	// now, create a new database
	if (!temp.createDatabase ("myDB", errMsg)) {
		cout << "Not able to create database: " + errMsg;
	} else {
		cout << "Created database.\n";
	}

	// now, create a new set in that database
	if (!temp.createSet <Supervisor> ("myDB", "mySet", errMsg)) {
		cout << "Not able to create set: " + errMsg;
	} else {
		cout << "Created set.\n";
	}

	//for (int num = 0; num < 25; ++num) {
		// now, create a bunch of data
		void *storage = malloc (1024 * 1024 * 8);
		{
			pdb :: makeObjectAllocatorBlock (storage, 1024 * 1024 * 8, true);

			pdb :: Handle <pdb :: Vector <pdb :: Handle <pdb :: Supervisor>>> supers = pdb :: makeObject <pdb :: Vector <pdb :: Handle<pdb :: Supervisor>>> ();

	        try {

	            for (int i = 0; true; i++) {

					Handle <Supervisor> super = makeObject <Supervisor> ("Steve Stevens", 20 + (i % 29));
					supers->push_back (super);
					for (int j = 0; j < 10; j++) {
						Handle <Employee> temp;
						if (i % 2 == 0)
							temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));
						else
							temp = makeObject <Employee> ("Albert Albertson", 20 + ((i + j) % 29));
						(*supers)[i]->addEmp (temp);
					}
				}

	        } catch ( pdb :: NotEnoughSpace &n ) {
	        	// we got here, so go ahead and store the vector
				if (!temp.storeData <Supervisor> (supers, "myDB", "mySet", errMsg)) {
					cout << "Not able to store data: " + errMsg;
					return 0;
				}	
				std :: cout << "stored the data!!\n";
	        }

		}
	//}

	// and shut down the server
	
	if (!temp.shutDownServer (errMsg))
		std :: cout << "Shut down not clean: " << errMsg << "\n";
	
        free (storage);

}

#endif

