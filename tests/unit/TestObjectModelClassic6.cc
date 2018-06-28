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

int main(int argc, char* argv[]) {

    //parse the parameters
    //parameter 1: number of objects

    if (argc <= 1) {
        std::cout << "Usage: #numObjects" << std::endl;
        exit(1);
    }

    int numObjects = atoi(argv[1]);

    // for timing
    auto begin = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numObjects; i++) {
        std::string* str = new std::string(
            "This is an object big enough to force flushing soon. This is an object big enough to "
            "force flushing soon. This is an object big enough to force flushing soon. This is an "
            "object big enough to force flushing soon. This is an object big enough to force "
            "flushing soon. This is an object big enough to force flushing soon. This is an object "
            "big enough to force flushing soon. This is an object big enough to force flushing "
            "soon. This is an object big enough to force flushing  soon. It has a total of 512 "
            "bytes to test. This is an object big enough to force flushing soon. This is an object "
            "big enough to force flushing soon. This is an object big enough to force flushing "
            "soon. This is an object big enough to force flushing soon. This is an object big "
            "enough to force flushing.." + std::to_string(i));
        delete str;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Duration to create all of the std::string objects: "
              << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << " ns."
              << std::endl;
};
