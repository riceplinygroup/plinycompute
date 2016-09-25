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

#ifndef TEST_38_CC
#define TEST_38_CC

#include "StorageClient.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "ChrisSelection.h"
#include "StringSelection.h"

// this won't be visible to the v-table map, since it is not in the biult in types directory
#include "SharedEmployee.h"

int main () {

	std:: cout << "Make sure to run bin/test601 or bin/test35 in a different window to provide a catalog/storage server.\n";

	// register the shared employee class
	pdb :: StorageClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"), true);

	string errMsg;
	if (!temp.registerType ("libraries/libSharedEmployee.so", errMsg)) {
		cout << "Not able to register type: " + errMsg;
	} else {
		cout << "Registered type.\n";
	}

        //to register selection type 
        temp.registerType ("libraries/libChrisSelection.so", errMsg);
        temp.registerType ("libraries/libStringSelection.so", errMsg);
	
}

#endif

