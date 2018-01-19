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
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <chrono>
#include <fcntl.h>
#include <string>

#include "InterfaceFunctions.h"
#include "Employee.h"
#include "Supervisor.h"

using namespace pdb;

int main() {

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    // load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, false);

    // get the file size
    std::ifstream in("testfile3", std::ifstream::ate | std::ifstream::binary);
    size_t fileLen = in.tellg();

    // read in the serialized record
    int filedesc = open("testfile3", O_RDONLY);
    Record<Vector<Handle<Supervisor>>>* myNewBytes =
        (Record<Vector<Handle<Supervisor>>>*)malloc(fileLen);
    // added by Jia to remove warnings
    size_t sizeRead = read(filedesc, myNewBytes, fileLen);
    if (sizeRead == 0) {
        std::cout << "Read failed" << std::endl;
    }
    close(filedesc);

    // get the root object
    Handle<Vector<Handle<Supervisor>>> mySupers = myNewBytes->getRootObject();

    // and loop through it, writing every 10th supervisor to a file

    // this is where we will store the individual records the we extract
    void* space = malloc(1024 * 1024 * 24);
    int filedescOut = open("testfile4", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);

    int numSupers = mySupers->size();
    for (int i = 0; i < numSupers; i += 10) {

        // get the requested supervisor
        Record<Supervisor>* mySuper =
            getRecord<Supervisor>((*mySupers)[i], space, 1024 * 1024 * 24);

        // and write to the file
        // added by Jia to remove warnings
        size_t sizeWritten = write(filedescOut, mySuper, mySuper->numBytes());
        if (sizeWritten == 0) {
            std::cout << "Write failed" << std::endl;
        }
    }

    // delete the RAM and close the file
    free(space);
    close(filedescOut);

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to write all of the objects: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;

    // get the file size
    std::ifstream in2("testfile4", std::ifstream::ate | std::ifstream::binary);
    fileLen = in2.tellg();
    std::cout << "Wrote " << fileLen << " output bytes.\n";
    free(myNewBytes);
}
