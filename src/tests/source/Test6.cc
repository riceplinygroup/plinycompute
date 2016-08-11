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

#include "InterfaceFunctions.h"
#include "PDBString.h"

using namespace pdb;

int main () {

	// for timing
	auto begin = std::chrono::high_resolution_clock::now();

	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, false);

	int i = 0;
        for (i = 0; i < 10000; i++) {
	    Handle <String> str = makeObject <String>("This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing  soon. It has a total of 512 bytes to test. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing soon. This is an object big enough to force flushing.."); 
        }

	auto end = std::chrono::high_resolution_clock::now();
	std::cout << "Duration to create all of the String objects: " <<
		std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << " ns." << std::endl;

};
	
