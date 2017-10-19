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

#include "PDBString.h"
#include "PDBMap.h"
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
#include <cassert>

using namespace pdb;

class Foo : public Object {

public:
    Handle<Map<String, String>> myMap;

    Foo() {
        myMap = makeObject<Map<String, String>>();
        String temp1("this is fun");
        String temp2("so is this");
        (*myMap)[temp1] = temp2;
    }

    ENABLE_DEEP_COPY
};

int main() {

    makeObjectAllocatorBlock(124 * 1024 * 1024, true);

    Map<int, Handle<Foo>> myMap;
    for (int i = 0; i < 1000; i++) {
        Handle<Foo> temp = makeObject<Foo>();
        myMap[i] = temp;
    }
    assert(myMap.size() == 1000);

    Handle<Map<int, Handle<int>>> anotherMap = makeObject<Map<int, Handle<int>>>();
    for (int i = 0; i < 1000; i++) {
        Handle<int> temp = makeObject<int>();
        *temp = i;
        (*anotherMap)[i] = temp;
    }
    assert(anotherMap->size() == 1000);

    for (auto& a : *anotherMap) {
        std::cout << "(" << a.key << ", " << *(a.value) << ") ";
    }

    /*	Vector <Handle <Foo>> myVec;
        for (int i = 0; i < 1000; i++) {
            Handle <Foo> temp = makeObject <Foo> ();
            myVec.push_back (temp);
        }

        Vector <Handle <Foo> > myVec2 (1000, 1000);
        for (int i = 0; i < 1000; i++) {
            myVec2 [i] = myVec [i];
        }
    */
}


#endif
