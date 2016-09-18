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

#ifndef TEST_27_CC
#define TEST_27_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "SharedEmployee.h"

// this won't be visible to the v-table map, since it is not in the built in types directory

int main (int argc, char * argv[]) {

	std :: cout << "Firstly, make sure to run bin/test23 in a different window to provide a catalog/storage server.\n";
        std :: cout << "Secondly, make sure to run bin/test24 first to register the type, create the database and set to add data to.\n";
        std :: cout << "Thirdly, make sure to run bin/test26 first to add data to the set created in last step.\n";
        std :: cout << "Now, you can run this test case:\n";
        std :: cout << std :: endl;
        std :: cout << std :: endl;
        std :: cout << "You can provide 2 arguments:" << std :: endl;
        std :: cout << "(1) the name of the database to fetch set from;" << std :: endl;
        std :: cout << "(2) the name of the set to fetch data from;"<< std :: endl;
        std :: cout << std :: endl;
        std :: cout << std :: endl; 
        std :: cout << "By default Test24_Database123 will be used for database name, Test24_Set123 will be used for set name\n";
        std :: cout << std :: endl;
        std :: cout << std :: endl;

        std :: string databaseName ("Test24_Database123");
        std :: string setName ("Test24_Set123");

        if (argc == 3) {
                databaseName = argv[1];
                setName = argv[2];
        }

        std :: cout << "to retrieve data from database with name: " << databaseName << std :: endl;
        std :: cout << "to retrieve data from set with name: " << setName << std :: endl;
        std :: cout << std :: endl;
        std :: cout << std :: endl;

        //start a client
        bool usePangea = true;
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), usePangea);
	string errMsg;
     
        // now, fetch data from the set specified
        temp.retrieveData<SharedEmployee>(databaseName, setName, errMsg);

        

}

#endif

