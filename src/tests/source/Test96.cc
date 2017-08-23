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
#include "IntDoubleVectorPair.h"

using namespace pdb;

int main () {


	// load up the allocator with RAM
	makeObjectAllocatorBlock (1024 * 1024 * 24, true);
        int numWord = 10;
        int numTopic = 4;

	//std :: cout << "ABOUT TO MAKE THE OBJECT.\n";
        Handle<Vector<Handle<IntDoubleVectorPair>>> result =
            makeObject<Vector<Handle<IntDoubleVectorPair>>>(numWord, numWord);

        for (int i = 0; i < numWord; i++) {
            Handle<Vector<double>> topicProb =
                makeObject<Vector<double>>(numTopic, numTopic);
            topicProb->fill(1.0);
            Handle<IntDoubleVectorPair> wordTopicProb=
                makeObject<IntDoubleVectorPair>(i, topicProb);
            (*result)[i] = wordTopicProb;
            std::cout << "The topic probability for word " << i << " : " << std::endl;
            ((*result)[i])->getVector().print();
        }
}
	
