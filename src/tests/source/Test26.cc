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

#ifndef TEST_26_CC
#define TEST_26_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "SharedEmployee.h"

// this won't be visible to the v-table map, since it is not in the built in types directory

int main (int argc, char * argv[]) {

	std :: cout << "Firstly, make sure to run bin/test23 in a different window to provide a catalog/storage server.\n";
        std :: cout << "Secondly, make sure to run bin/test24 first to register the type, create the database and set to add data to.\n";
        std :: cout << std :: endl;
        std :: cout << std :: endl;
        std :: cout << "You can provide 3 arguments:" << std :: endl;
        std :: cout << "(1) the name of the database to add data to;" << std :: endl;
        std :: cout << "(2) the name of the set to add data to;"<< std :: endl;
        std :: cout << "(3) the size of data to add (in MB) ;"<< std :: endl;
        std :: cout << std :: endl;
        std :: cout << std :: endl; 
        std :: cout << "By default Test24_Database123 will be used for database name, Test24_Set123 will be used for set name, and 128MB will be used for data size.\n";
        std :: cout << std :: endl;
        std :: cout << std :: endl;

        std :: string databaseName ("Test24_Database123");
        std :: string setName ("Test24_Set123");

        int numOfMb = 128;
        if (argc == 4) {
                databaseName = argv[1];
                setName = argv[2];
                numOfMb = atoi(argv[3]);
        }

        std :: cout << "to add database with name: " << databaseName << std :: endl;
        std :: cout << "to add set with name: " << setName << std :: endl;
        std :: cout << "to add data with size: " << numOfMb << "MB" << std :: endl;
        std :: cout << std :: endl;
        std :: cout << std :: endl;

        //start a client
        bool usePangea = true;
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), usePangea);
	string errMsg;
     

        // now, create a bunch of data
        size_t dataSize = numOfMb * 1024 * 1024;
        int numIterations = dataSize/(128 * 1024);
        for (int i = 0; i < numIterations; i++) {
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
                                if (!temp.storeData <SharedEmployee> (storeMe, databaseName, setName, errMsg, false)) {
                                         cout << "Not able to store data: " + errMsg;
                                         return 0;
                                }
                                //std :: cout << "stored the data!!\n";
                        }
                 }
                 free (storage);
        }

        

}

#endif

