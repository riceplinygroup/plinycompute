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
#include "Employee.h"
#include "Supervisor.h"

using namespace pdb;

int main () {

	// for timing
	auto begin = std::chrono::high_resolution_clock::now();

	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, false);

	// get the file size
	std::ifstream in ("testfile", std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg(); 

	// read in the serialized record
	int filedesc = open ("testfile", O_RDONLY);
	Record <Vector <Handle <Supervisor>>> *myNewBytes = (Record <Vector <Handle <Supervisor>>> *) malloc (fileLen);
	size_t sizeRead = read (filedesc, myNewBytes, fileLen);
        if (sizeRead == 0) {
           std :: cout << "Read failed." << std :: endl;
        }
	// get the root object
	Handle <Vector <Handle <Supervisor>>> mySupers = myNewBytes->getRootObject ();

	// and loop through it, copying over some employees
	int numSupers = (*mySupers).size ();
	Handle <Vector <Handle <Employee>>> result = makeObject <Vector <Handle <Employee>>> (10);
	for (int i = 0; i < numSupers; i++) {
		result->push_back (((*mySupers)[i]->getEmp (i % 10)));
	}

	// now, we serialize those employees
	close (filedesc);

	Record <Vector <Handle <Employee>>> *myBytes = getRecord <Vector <Handle <Employee>>> (result);
	filedesc = open ("testfile2", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	size_t sizeWritten = write (filedesc, myBytes, myBytes->numBytes ());
        if (sizeWritten == 0) {
           std :: cout << "Write failed." << std :: endl;
        }
	close (filedesc);
	std :: cout << "Wrote " << myBytes->numBytes () << " bytes to the file.\n";

	auto end = std::chrono::high_resolution_clock::now();
	std::cout << "Duration to do the copies and then write the new objects: " <<
		std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
	std :: cout << "Are " << getBytesAvailableInCurrentAllocatorBlock () << " bytes left in the current allocation block.\n";

	for (int i = 0; i < numSupers; i += 1000) {
		(*result)[i]->print ();
		std :: cout << "\n";
	}

	free (myNewBytes);
}
	
