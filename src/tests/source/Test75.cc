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
#ifndef TEST_75_CC
#define TEST_75_CC


// by Jia, May 2017

#include <cstddef>
#include <iostream>
#include "Ptr.h"
#include "ComputeSource.h"
#include "ComputeSink.h"
#include "ComputeExecutor.h"
#include "InterfaceFunctions.h"
#include "TupleSpec.h"
#include "TupleSet.h"
#include "TupleSetMachine.h"
#include "JoinTuple.h"
#include "PDBString.h"

using namespace pdb;


int main(int argc, char* argv[]) {

    void* myBlock = (void*)malloc(24 * 1024 * 1024);

    makeObjectAllocatorBlock(myBlock, 24 * 1024 * 1024, true);

    Handle<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>> myMap =
        makeObject<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>>();

    JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>& temp = myMap->push(8);
    Handle<String> str = makeObject<String>("abcdefg");
    copyFrom(temp.myData, str);
    Handle<int> myInt = makeObject<int>(378);
    copyFrom(temp.myOtherData.myData, myInt);
    Handle<double> myDouble = makeObject<double>(3.3);
    copyFrom(temp.myOtherData.myOtherData.myData, myDouble);


    JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>& temp1 = myMap->push(27);
    str = makeObject<String>("hijklmnop");
    copyFrom(temp1.myData, str);
    myInt = makeObject<int>(134);
    copyFrom(temp1.myOtherData.myData, myInt);
    myDouble = makeObject<double>(16.7);
    copyFrom(temp1.myOtherData.myOtherData.myData, myDouble);

    myMap->setObjectSize();

    getRecord(myMap);

    void* myOtherBlock = (void*)malloc(24 * 1024 * 1024);
    memcpy(myOtherBlock, myBlock, 24 * 1024 * 1024);

    Handle<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>> myCopiedMap =
        ((Record<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>>*)
             myOtherBlock)
            ->getRootObject();
    const size_t objectSize = myCopiedMap->getObjectSize();
    std::cout << "Received map size: " << myCopiedMap->size() << std::endl;
    std::cout << "map object size: " << objectSize << std::endl;

    makeObjectAllocatorBlock(24 * 1024 * 1024, true);
    Handle<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>> myNewMap =
        makeObject<JoinMap<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>>();
    for (JoinMapIterator<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>> iter =
             myCopiedMap->begin();
         iter != myCopiedMap->end();
         ++iter) {
        JoinRecordList<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>* myList =
            *iter;
        size_t mySize = myList->size();
        size_t myHash = myList->getHash();
        std::cout << "list size: " << mySize << std::endl;
        std::cout << "list hash: " << myHash << std::endl;
        for (int i = 0; i < mySize; i++) {
            JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>& temp =
                myNewMap->push(myHash);
            packData(temp, (*myList)[i]);
        }
        free(myList);
    }

    std::cout << "New map size: " << myNewMap->size() << std::endl;
    for (JoinMapIterator<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>> iter =
             myNewMap->begin();
         iter != myNewMap->end();
         ++iter) {
        JoinRecordList<JoinTuple<String, JoinTuple<int, JoinTuple<double, char[0]>>>>* myList =
            *iter;
        size_t mySize = myList->size();
        size_t myHash = myList->getHash();
        std::cout << "list size: " << mySize << std::endl;
        std::cout << "list hash: " << myHash << std::endl;
        for (int i = 0; i < mySize; i++) {
            std::cout << "String is " << (*myList)[i].myData << std::endl;
            std::cout << "int is " << (*myList)[i].myOtherData.myData << std::endl;
            std::cout << "double is " << (*myList)[i].myOtherData.myOtherData.myData << std::endl;
        }
        delete (myList);
    }
}


#endif
