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

#ifndef TEST_39_H
#define TEST_39_H

#include "Join.h"
#include "PDBString.h"
#include "Query.h"
#include "Lambda.h"
#include "Selection.h"
#include "QueryClient.h"
#include "QueryOutput.h"
#include "StorageClient.h"
#include "ChrisSelection.h"
#include "StringSelection.h"
#include "SharedEmployee.h"

using namespace pdb;

int main (int argc, char * argv[]) {
        if (argc != 2) {
              std :: cout << "[Usage] bin/test39 ip_address_of_remote_node" << std :: endl;
        }
        string address = argv[1];

	// for allocations
	const UseTemporaryAllocationBlock tempBlock {1024 * 128};

        string errMsg;
        PDBLoggerPtr myLogger = make_shared <pdb :: PDBLogger> ("clientLog");


	// connect to the query client
	QueryClient myClient (8108, address, myLogger, true);


	// print the resuts
	SetIterator <String> result = myClient.getSetIterator <String> ("chris_db", "output_set1");
	std :: cout << "First set of query results: ";
	for (auto a : result) 
		std :: cout << (*a) << "; ";
	std :: cout << "\n\nSecond set of query results: ";
	result = myClient.getSetIterator <String> ("chris_db", "output_set2");
	for (auto a : result) 
		std :: cout << (*a) << "; ";
	std :: cout << "\n";

        std :: cout << "to delete results and so files" << std :: endl;	
	// and delete the sets
	myClient.deleteSet ("chris_db", "output_set1");
	myClient.deleteSet ("chris_db", "output_set2");
        system ("scripts/cleanupSoFiles.sh");
        
}

#endif
