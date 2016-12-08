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

// this won't be visible to the v-table map, since it is not in the biult in types directory
#include "SharedEmployee.h"

int main (int argc, char * argv[]) {

        int numOfMb = 128;

	std:: cout << "Make sure to run bin/test603 in a different window to provide a catalog/storage server.\n";
        std :: cout << "You can provide one parameter as the size of data to add (in MB)"<< std :: endl;

        if (argc >1) {
            numOfMb = atoi(argv[1]);
        }

        std :: cout << "to add data with size: " << numOfMb << "MB" << std :: endl;

        bool clusterMode = false;
        if (argc > 2) {
             clusterMode = true;
             std :: cout << "We are running in cluster mode" << std :: endl;
        }
        else {
             std :: cout << "We are not running in cluster mode, if you want to run in cluster mode, please provide any character as second parameter" << std :: endl;
        }

	// register the shared employee class
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), true);

	string errMsg;
        
	if (!temp.registerType ("libraries/libSharedEmployee.so", errMsg)) {
		cout << "Not able to register type: " + errMsg;
                return -1;
	} else {
		cout << "Registered type.\n";
	}
        
        //to register selection type
        if (clusterMode == true) {
            temp.registerType ("libraries/libChrisSelection.so", errMsg);
            temp.registerType ("libraries/libStringSelection.so", errMsg);
        }


	// now, create a new database
	if (!temp.createDatabase ("chris_db", errMsg)) {
		cout << "Not able to create database: " + errMsg;
                return -1;
	} else {
		cout << "Created database.\n";
	}

	// now, create a new set in that database
	if (!temp.createSet <SharedEmployee> ("chris_db", "chris_set", errMsg)) {
		cout << "Not able to create set: " + errMsg;
                return -1;
	} else {
		cout << "Created set.\n";
	}

        int numIterations = numOfMb;
        int total = 0;
	for (int num = 0; num < numIterations; ++num) {
		// now, create a bunch of data
			pdb :: makeObjectAllocatorBlock (1024 * 1024 * 1, true);
			pdb :: Handle <pdb :: Vector <pdb :: Handle <SharedEmployee>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle <SharedEmployee>>> ();
			int i;
			try {
		
				for (i = 0; true; i++) {
					pdb :: Handle <SharedEmployee> myData = pdb :: makeObject <SharedEmployee> ("Joe Johnson" + to_string (i), i + 45);	
					storeMe->push_back (myData);
                                        total++;
				}
		
			} catch (pdb :: NotEnoughSpace &n) {
		
				// we got here, so go ahead and store the vector
				if (!temp.storeData <SharedEmployee> (storeMe, "chris_db", "chris_set", errMsg)) {
					cout << "Not able to store data: " + errMsg;
					return -1;
				}	
				std :: cout << i << std::endl;
				std :: cout << "stored the data!!\n";
			}
	}

	// and shut down the server
        temp.flushData(errMsg);
/*        	
	if (!temp.shutDownServer (errMsg))
		std :: cout << "Shut down not clean: " << errMsg << "\n";
*/	
        std :: cout << "count=" << total << std :: endl;
        return 0;
}

#endif

