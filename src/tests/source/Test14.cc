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

#ifndef TEST_14_CC
#define TEST_14_CC

#include "CatalogClient.h"

// this won't be visible to the v-table map, since it is not in the biult in types directory
#include "../../sharedLibraries/source/SharedLibEmployee.cc"

int main () {

	std:: cout << "Make sure to run bin/test15 in a different window!!\n";

	// register the shared employee class
	pdb :: CatalogClient temp (8108, "localhost", make_shared <pdb :: PDBLogger> ("clientLog"));
	if (!temp.registerType ("libraries/libSharedEmployee.so"))
		cout << "Not able to register type.\n";

	pdb :: makeObjectAllocatorBlock (8192 * 1024, true);

	// since SharedEmployee is not a builtin object, this will cause a request to the catalog to 
	// obtain the type code for the SharedEmployee class
	pdb :: Handle <PrintableObject> myData = pdb :: makeObject <SharedEmployee> ("Joe Johnson", 12);

	// and here, we'll need to fix the vTable for myData, using the shared library from the catalog
	myData->print ();	
	std :: cout << "\n";

	// and downcast
	pdb :: Handle <SharedEmployee> newOne = pdb :: unsafeCast <SharedEmployee> (myData);
	std :: cout << *(newOne->getName ()) << "\n";	
}

#endif

