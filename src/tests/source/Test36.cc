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

#ifndef TEST_36_CC
#define TEST_36_CC

#include "PDBMap.h"
#include "PDBString.h"
#include "Employee.h"
#include "InterfaceFunctions.h"

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

using namespace pdb;

int main () {

	makeObjectAllocatorBlock (124 * 1024 * 1024, true);

	Handle <Map <int, int>> myMap = makeObject <Map <int, int>> ();

	for (int i = 0; i < 100; i++) {
		(*myMap)[i] = i + 120;
	}

	for (int i = 0; i < 100; i++) {
		std :: cout << (*myMap)[i] << " ";
	}
	std :: cout << "\n";

	Handle <Map <String, int>> myOtherMap = makeObject <Map <String, int>> ();

	for (int i = 0; i < 100; i++) {
		String temp (std :: to_string (i) + std :: string (" is my number"));
		(*myOtherMap)[temp] = i;
	}

	for (int i = 0; i < 100; i++) {
		String temp (std :: to_string (i) + std :: string (" is my number"));
		std :: cout << (*myOtherMap)[temp] << " ";
	}
	std :: cout << "\n";

	Handle <Map <Handle <String>, Handle <Employee>>> anotherMap = makeObject <Map <Handle <String>, Handle <Employee>>> ();

	for (int i = 0; i < 100; i++) {
		Handle <String> empName = makeObject <String> (std :: to_string (i) + std :: string (" is my number"));
		Handle <Employee> myEmp = makeObject <Employee> ("Joe Johnston " + std :: to_string (i), i);
		(*anotherMap)[empName] = myEmp;
	}

	for (int i = 0; i < 100; i++) {
		Handle <String> empName = makeObject <String> (std :: to_string (i) + std :: string (" is my number"));
		(*anotherMap)[empName]->print ();
		std :: cout << " ";
	}
	std :: cout << "\n";	        

	int filedesc = open ("testfile", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	Record <Map <Handle <String>, Handle <Employee>>> *myBytes = getRecord <Map <Handle <String>, Handle <Employee>>> (anotherMap);
	write (filedesc, myBytes, myBytes->numBytes ());
	close (filedesc);
	std :: cout << "Wrote " << myBytes->numBytes () << " bytes to the file.\n";

	std::ifstream in ("testfile", std::ifstream::ate | std::ifstream::binary);
	size_t fileLen = in.tellg();
	filedesc = open ("testfile", O_RDONLY);
	myBytes = (Record <Map <Handle <String>, Handle <Employee>>> *) malloc (fileLen);
	read (filedesc, myBytes, fileLen);	
	close (filedesc);
	anotherMap = myBytes->getRootObject ();

	for (int i = 0; i < 100; i++) {
		Handle <String> empName = makeObject <String> (std :: to_string (i) + std :: string (" is my number"));
		(*anotherMap)[empName]->print ();
		std :: cout << " ";
	}
	std :: cout << "\n";
	free (myBytes);
}

#endif

