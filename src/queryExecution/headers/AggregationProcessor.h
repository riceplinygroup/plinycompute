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

//by Jia, Mar 13 2017

#include "UseTemporaryAllocationBlock.h"
#include "InterfaceFunctions.h"
#include "PDBMap.h"
#include "PDBVector.h"
#include "Handle.h"
#include "SimpleSingleTableQueryProcessor.h"

namespace pdb {

template <class KeyType, class ValueType>
class AggregationProcessor : public SimpleSingleTableQueryProcessor {

public:

    ~AggregationProcessor () {};
    AggregationProcessor () {};
    AggregationProcessor (HashPartitionID id);   
    void initialize () override;
    void loadInputPage (void * pageToProcess) override;
    void loadOutputPage (void * pageToWriteTo, size_t numBytesInPage) override;
    bool fillNextOutputPage () override;
    void finalize () override;
    void clearOutputPage () override;
    void clearInputPage () override;
    bool needsProcessInput() override;

private:

    UseTemporaryAllocationBlockPtr blockPtr;
    Handle <Map <KeyType, ValueType>> inputData;
    Handle <Map <KeyType, ValueType>> outputData;
    bool finalized;
    Handle<Map<KeyType, ValueType>> curMap;
    int id;
    
    //the iterators for current map partition
    PDBMapIterator <KeyType, ValueType> begin;
    PDBMapIterator <KeyType, ValueType> end;

};

}


#include "AggregationProcessor.cc"


#endif
