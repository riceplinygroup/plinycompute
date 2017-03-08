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

#define NUM_OBJECTS 10371

#include <cstddef>
#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <unistd.h>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctime>
#include <string>

#include "Handle.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "Employee.h"
#include "Supervisor.h"
#include "ZB_Company.h"

using namespace pdb;

int main () {

	// for timing
	auto begin = std::chrono::high_resolution_clock::now();

	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, true);

	Handle <ZB_Company> myCompany = makeObject <ZB_Company> ();
	for (int i = 0; i < 5; i++) {
		
		Supervisor tempSup ("Really cool name", 23 + i);
		for (int j = 0; j < 10; j++) {
			Handle <Employee> temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));	
			tempSup.addEmp (temp);
		}

		myCompany->addSupervisor (getHandle (tempSup), i);	
	}

	myCompany->print ();

	auto end = std::chrono::high_resolution_clock::now();
	std::cout << "Duration to create all of the objects: " <<
		std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;

	begin = std::chrono::high_resolution_clock::now();

	auto myBytes = getRecord (myCompany);
	int filedesc = open ("testfile7", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	write (filedesc, myBytes, myBytes->numBytes ());
	close (filedesc);

	end = std::chrono::high_resolution_clock::now();
	std::cout << "Duration to write the objects to a file: " <<
		std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;
	std :: cout << "Wrote " << myBytes->numBytes () << " bytes to the file.\n";

	std :: cout << "Are " << getNumObjectsInCurrentAllocatorBlock () << " objects in current block.\n";
	myCompany = nullptr;
	std :: cout << "After assign, are " << getNumObjectsInCurrentAllocatorBlock () << " objects in current block.\n";

}
