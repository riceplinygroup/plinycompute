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

#define NUM_OBJECTS 12000

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

using namespace pdb;

int main() {

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    // load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, true);

    Handle<String> temp = makeObject<String>("this is soooooo cool");
    Handle<String> temp2 = makeObject<String>("this");
    Handle<String> temp3;
    temp3 = temp;
    std::cout << *temp << " " << *temp2 << " " << *temp3 << "\n";
    temp = temp2 = temp3 = nullptr;

    int i = 0;
    Handle<Vector<Handle<Supervisor>>> supers = makeObject<Vector<Handle<Supervisor>>>(10);
    try {
        // put a lot of copies of it into a vector
        for (; true; i++) {

            supers->push_back(makeObject<Supervisor>("Joe Johnson", 20 + (i % 29)));
            for (int j = 0; j < 10; j++) {
                Handle<Employee> temp = makeObject<Employee>("Steve Stevens", 20 + ((i + j) % 29));
                (*supers)[supers->size() - 1]->addEmp(temp);
            }
            if (i > NUM_OBJECTS) {
                break;
            }
        }

    } catch (NotEnoughSpace& e) {
        std::cout << "Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create all of the objects: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

    for (int i = 0; i < NUM_OBJECTS; i += 1000) {
        (*supers)[i]->print();
    }

    begin = std::chrono::high_resolution_clock::now();

    Record<Vector<Handle<Supervisor>>>* myBytes = getRecord<Vector<Handle<Supervisor>>>(supers);
    int filedesc = open("testfile", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    size_t sizeWritten = write(filedesc, myBytes, myBytes->numBytes());
    if (sizeWritten == 0) {
        std::cout << "Write failed" << std::endl;
    }
    close(filedesc);

    end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write the objects to a file: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;
    std::cout << "Wrote " << myBytes->numBytes() << " bytes to the file.\n";

    std::cout << "Are " << getNumObjectsInCurrentAllocatorBlock() << " objects in current block.\n";
    supers = nullptr;
    std::cout << "After assign, are " << getNumObjectsInCurrentAllocatorBlock()
              << " objects in current block.\n";
}
