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
#include <fstream>
#include "Handle.h"
#include "PDBVector.h"
#include "InterfaceFunctions.h"
#include "JoinMap.h"
#include "Configuration.h"
using namespace pdb;

int main(int argc, char* argv[]) {

    std::string fileName = argv[1];
    std::string type = argv[2];
    FILE* myFile = fopen(fileName.c_str(), "r");
    char* buffer = (char*)malloc(DEFAULT_NET_PAGE_SIZE * sizeof(char));
    fread(buffer, 1, DEFAULT_NET_PAGE_SIZE, myFile);
    if (type == "broadcast") {
        Handle<JoinMap<Object>> myMap = ((Record<JoinMap<Object>>*)buffer)->getRootObject();
        std::cout << "broadcasted map size is " << myMap->size() << std::endl;
    } else {
        Handle<Vector<Handle<JoinMap<Object>>>> myMaps =
            ((Record<Vector<Handle<JoinMap<Object>>>>*)buffer)->getRootObject();
        std::cout << "number of partitioned maps is " << myMaps->size() << std::endl;
        for (int i = 0; i < myMaps->size(); i++) {
            std::cout << "the " << i << "-th partitioned map size is " << (*myMaps)[i]->size()
                      << std::endl;
        }
    }
}
