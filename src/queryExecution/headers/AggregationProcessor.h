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
#ifndef AGGREGATION_PROCESSOR_H
#define AGGREGATION_PROCESSOR_H

#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBMap.h"
#include "PDBVector.h"
#include "Handle.h"

namespace pdb {

template <class KeyType, class ValueType>
class AggregationProcessor {

public:

    ~AggregationProcessor () {};
    AggregationProcessor ();
    void initialize ();
    void loadInputPage (void * pageToProcess);
    void loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) override;
    bool fillNextOutputPage ();
    void finalize ();
    void clearOutputPage ();
    void clearInputPage ();

private:

    UseTemporaryAllocationBlockPtr blockPtr;
    Handle <Vector<Handle <Map <KeyType, ValueType>>>> inputData;
    Handle <Map <KeyType, ValueType>> outputData;
    bool finalized;
    int curVecPos;
    Handle<Map<KeyType, ValueType>> curMap;
    
    //the iterators for current map partition
    PDBMapIterator <KeyType, ValueType> begin;
    PDBMapIterator <KeyType, ValueType> end;

};

}


#include "AggregationProcessor.cc"


#endif
