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

#include <cstddef>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <ctime>
#include <chrono>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>

#include "InterfaceFunctions.h"
#include "CatalogClient.h"
#include "PDBVector.h"
#include "Record.h"
#include "Handle.h"
#include "../../sharedLibraries/source/SharedLibEmployee.cc"

using namespace pdb;

int main () {

	CatalogClient temp (8108, "localhost", std :: make_shared <PDBLogger> ("clientLog"));
	std :: cout << "Make sure to run bin/test17 first to create a database file.\n";
	std :: cout << "Make sure to run bin/test15 separately in a different window to get a catalog server.\n";
	std :: cout << "\tThe latter is needed to load the SharedLibEmployee class dynamically.\n";

	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, false);

	// read in the first page in the file
	int filedesc = open ("StorageDir/chris_db.chris_set", O_RDONLY);
	void *ram = malloc (1024 * 128);
	read (filedesc, ram, 1024 * 128);

	Record <Vector <Handle <SharedEmployee>>> *myData = 
		(Record <Vector <Handle <SharedEmployee>>> *) ram;

	Handle <Vector <Handle <SharedEmployee>>> data = myData->getRootObject ();
	for (int i = 0; i < 5; i++) {
		(*data)[i]->print ();
		std :: cout << "\n";
	}
	
	close (filedesc);
	free (ram);

}
	
