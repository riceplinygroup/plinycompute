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

int main() {

    // load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, false);

    // get the file size
    std::ifstream in("testfile6", std::ifstream::ate | std::ifstream::binary);
    size_t fileLen = in.tellg();

    // read in the serialized record
    int filedesc = open("testfile6", O_RDONLY);
    Record<Vector<Vector<Employee>>>* myNewBytes =
        (Record<Vector<Vector<Employee>>>*)malloc(fileLen);
    size_t sizeRead = read(filedesc, myNewBytes, fileLen);
    if (sizeRead == 0) {
        std::cout << "Read failed" << std::endl;
    }
    // get the root object
    Handle<Vector<Vector<Employee>>> mySupers = myNewBytes->getRootObject();

    // and extract handles to each of the vectors
    for (int i = 0; i < 100; i++) {

        Handle<Vector<Employee>> temp = getHandle<Vector<Employee>>((*mySupers)[i]);
        Employee tempEmp("And here is one more.", 34);
        temp->push_back(tempEmp);

        for (int j = 0; j < 101; j++) {
            (*temp)[j].print();
            std::cout << "\n";
        }
    }

    delete (myNewBytes);
    close(filedesc);
}
