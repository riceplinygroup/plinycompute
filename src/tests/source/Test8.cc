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
#include <sstream> 
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

using namespace pdb;

int main () {

        // for timing
        auto begin = std::chrono::high_resolution_clock::now();

	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, false);

	Handle <Vector <Vector <Employee>>> data = makeObject <Vector <Vector <Employee>>> ();

	for (int i = 0; i < 100; i++) {

		Vector <Employee> temp;
		for (int j = 0; j < 100; j++) {
			std::stringstream ss;
			ss << "myEmployee " << i << ", " << j;
			Employee temp2 (ss.str (), j);
			temp.push_back (temp2);
		}

		data->push_back (temp);
	}	

        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Duration to create all of the objects: " <<
                std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;

        std :: cout << "Are " << getBytesAvailableInCurrentAllocatorBlock () << " bytes left in the current allocation block.\n";

        Record <Vector <Vector <Employee>>> *myBytes = getRecord <Vector <Vector <Employee>>> (data);
        int filedesc = open ("testfile6", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
        write (filedesc, myBytes, myBytes->numBytes ());
        close (filedesc);
}
	
