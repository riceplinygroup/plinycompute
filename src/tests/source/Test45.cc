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
#include "UseTemporaryAllocationBlock.h"

using namespace pdb;

int main () {

  int j= 0;
  while (j < 50) {
	// load up the allocator with RAM
        char * storage = (char *) malloc (1024 * 1024 * 24);
	makeObjectAllocatorBlock (storage, 1024 * 1024 * 24, true);
	int i = 0;
	Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> (10);
	try {
		// put a lot of copies of it into a vector
		for (i = 0; i < 10000; i++) {
	
			supers->push_back (makeObject <Supervisor> ("Joe Johnson", 20 + (i % 29)));
			for (int j = 0; j < 10; j++) {
				Handle <Employee> temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));	
				(*supers)[supers->size () - 1]->addEmp (temp);
			}
			if (i > NUM_OBJECTS) {
				break;
			}
		}

	} catch (NotEnoughSpace &e) {
		std :: cout << "1: Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
	}
        std :: cout << "1: inserted " << i << "objects" << std :: endl;
        getRecord(supers);
        //free(storage);

        {UseTemporaryAllocationBlock myBlock {1024*1024};
        Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> (10);
        try {
                // put a lot of copies of it into a vector
                for (i = 0; i < 200; i++) {

                        supers->push_back (makeObject <Supervisor> ("Joe Johnson", 20 + (i % 29)));
                        for (int j = 0; j < 10; j++) {
                                Handle <Employee> temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));
                                (*supers)[supers->size () - 1]->addEmp (temp);
                        }
                        if (i > NUM_OBJECTS) {
                                break;
                        }
                }

        } catch (NotEnoughSpace &e) {
                std :: cout << "2: Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
        }}

        std :: cout << "2: inserted " << i << "objects" << std :: endl;

        {UseTemporaryAllocationBlock myBlock {1024*1024};
        Handle <Vector <Handle <Supervisor>>> supers = makeObject <Vector <Handle <Supervisor>>> (10);
        try {
                // put a lot of copies of it into a vector
                for (i = 0; i < 200; i++) {

                        supers->push_back (makeObject <Supervisor> ("Joe Johnson", 20 + (i % 29)));
                        for (int j = 0; j < 10; j++) {
                                Handle <Employee> temp = makeObject <Employee> ("Steve Stevens", 20 + ((i + j) % 29));
                                (*supers)[supers->size () - 1]->addEmp (temp);
                        }
                        if (i > NUM_OBJECTS) {
                                break;
                        }
                }

        } catch (NotEnoughSpace &e) {
                std :: cout << "3: Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
        }}

        std :: cout << "3: inserted " << i << "objects" << std :: endl;
        free(storage);
        j++;
    }
}
	
