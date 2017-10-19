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

#ifndef Object_Model_Test1_CC
#define Object_Model_Test1_CC


#define NUM_OBJECTS 10371
#include <stdio.h>
#include <cstddef>
#include <iostream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <cstring>
#include <unistd.h>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <fstream>
#include <unistd.h>

#include <qunit.h>


#include "Handle.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "Employee.h"
#include "Supervisor.h"

using namespace pdb;

using QUnit::UnitTest;


/**
 * This test generates lots of Supervisor and Employee objects and write them to a file.
 * Then it reads the file back and reads from file the generated objects.
 * It tests the Object model functionality and PDB Vector.
 */


int main() {
    QUnit::UnitTest qunit(std::cerr, QUnit::verbose);


    // Create Objects and write them into a file.
    // 1. load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, true);

    // create objects and write into a file
    int i = 0;
    Handle<Vector<Handle<Supervisor>>> supers = makeObject<Vector<Handle<Supervisor>>>(10);
    try {
        // put a lot of copies of it into a vector
        for (; true; i++) {

            supers->push_back(makeObject<Supervisor>("Joe Johnson", i));
            for (int j = 0; j < 10; j++) {
                Handle<Employee> temp = makeObject<Employee>("Steve Stevens", (i + j));
                (*supers)[supers->size() - 1]->addEmp(temp);
            }
            if (i > NUM_OBJECTS) {
                break;
            }
        }

    } catch (NotEnoughSpace& e) {
        std::cout << "Finally, after " << i << " inserts, ran out of RAM for the objects.\n";
    }

    Record<Vector<Handle<Supervisor>>>* myBytes = getRecord<Vector<Handle<Supervisor>>>(supers);

    int fileDesc = open("testfile", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
    // added by Jia
    ssize_t sizeWritten = write(fileDesc, myBytes, myBytes->numBytes());
    if (sizeWritten == 0) {
        std::cout << "Write failed" << std::endl;
    }
    close(fileDesc);


    // Start of the Second Phase - Reading File Back
    // 2.  read objects back from a file.

    // load up the allocator with RAM
    makeObjectAllocatorBlock(1024 * 1024 * 24, false);

    // get the file size
    std::ifstream in2("testfile", std::ifstream::ate | std::ifstream::binary);
    ssize_t fileLen2 = in2.tellg();

    // read in the serialized record
    int filedesc2 = open("testfile", O_RDONLY);

    auto * myNewBytes = (Record<Vector<Handle<Supervisor>>>*)malloc(static_cast<size_t>(fileLen2));
    ssize_t sizeRead = read(filedesc2, myNewBytes, static_cast<size_t>(fileLen2));
    // added by Jia
    if (sizeRead == 0) {
        std::cout << "Read failed" << std::endl;
    }
    // get the root object
    Handle<Vector<Handle<Supervisor>>> mySupers = myNewBytes->getRootObject();

    // and loop through it, copying over some employees
    size_t numSupers = (*mySupers).size();

    Handle<Vector<Handle<Employee>>> result = makeObject<Vector<Handle<Employee>>>(10);
    for (int k = 0; k < numSupers; k++) {
        result->push_back((*mySupers)[k]->getEmp(k % 10));
    }

    // now, we serialize those employees
    close(filedesc2);

    for (int k = 0; k < numSupers; k += 1000) {
        QUNIT_IS_EQUAL(k, (*result)[k]->getAge());
    }

    // free memory
    free(myNewBytes);

    // remove the file
    if (remove("testfile") != 0)
        perror("Error deleting file");
    else
        puts("File successfully deleted");
}
#endif
