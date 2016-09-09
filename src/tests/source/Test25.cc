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

#ifndef TEST_24_CC
#define TEST_24_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "../../sharedLibraries/source/SharedLibEmployee.cc"

// this won't be visible to the v-table map, since it is not in the built in types directory

int main (int argc, char * argv[]) {

	std :: cout << "Make sure to run bin/test23 in a different window to provide a catalog/storage server.\n";
        std :: cout << "You can provide two argument as the name of the database to add, and name of the set to add\n"; 
        std :: cout << "By default Test24_Database123 will be used for database name, and Test24_Set123 will be used for set name.\n";
        std :: string databaseName ("Test24_Database123");
        std :: string setName ("Test24_Set123");
        if (argc ==3) {
                databaseName = argv[1];
                setName = argv[2];
        }
        std :: cout << "to add database with name: " << databaseName << std :: endl;
        std :: cout << "to add set with name: " << setName << std :: endl;

        //start a client
        bool usePangea = true;
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), usePangea);
	string errMsg;

        // register the shared employee class
        if (!temp.registerType ("libraries/libSharedEmployee.so", errMsg)) {
                cout << "Not able to register type: " + errMsg;
        } else {
                cout << "Registered type.\n";
        }

	// now, create a new database
	if (!temp.createDatabase (databaseName, errMsg)) {
		std :: cout << "Not able to create database: " + errMsg;
                std :: cout << "Please change a database name, or remove the pdbRoot AND CatalogDir directories at where you run test23";
	} else {
		std :: cout << "Created database.\n";
	}

        // now create a new set in that database
        if (!temp.createSet<SharedEmployee> (databaseName, setName, errMsg)) {
                std :: cout << "Not able to set: " + errMsg;
                std :: cout << "Please change a set name, or remove the pdbRoot AND CatalogDir directories at where you run test23";
        } else {
                std :: cout << "Created set.\n";
        }

        // now, create a bunch of data
        void *storage = malloc (128 * 1024);
        {
                pdb :: makeObjectAllocatorBlock (storage, 128 * 1024, true);
                pdb :: Handle <pdb :: Vector <pdb :: Handle <SharedEmployee>>> storeMe = pdb :: makeObject <pdb :: Vector <pdb :: Handle <SharedEmployee>>> ();

                try {

                        for (int i = 0; true; i++) {
                                pdb :: Handle <SharedEmployee> myData = pdb :: makeObject <SharedEmployee> ("Joe Johnson" + to_string (i), i + 45);
                                storeMe->push_back (myData);
                        }

                } catch (pdb :: NotEnoughSpace &n) {

                        // we got here, so go ahead and store the vector
                        if (!temp.storeData <SharedEmployee> (storeMe, databaseName, setName, errMsg)) {
                                cout << "Not able to store data: " + errMsg;
                                return 0;
                        }
                        std :: cout << "stored the data!!\n";
                }
        }
        free (storage);


}

#endif

